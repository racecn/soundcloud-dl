import { SoundCloudApi, StreamDetails, Track } from "./soundcloudApi";
import { Logger } from "./utils/logger";
import {
  onBeforeSendHeaders,
  onBeforeRequest,
  downloadToFile,
  onMessage,
  onPageActionClicked,
  openOptionsPage,
  getExtensionManifest,
  sendMessageToTab,
} from "./compatibilityStubs";
import { MetadataExtractor, ArtistType, RemixType } from "./metadataExtractor";
import { Mp3TagWriter } from "./tagWriters/mp3TagWriter";
import { loadConfiguration, storeConfigValue, getConfigValue, registerConfigChangeHandler } from "./utils/config";
import { TagWriter } from "./tagWriters/tagWriter";
import { Mp4TagWriter } from "./tagWriters/mp4TagWriter";
import { Parser } from "m3u8-parser";
import { concatArrayBuffers, sanitizeFilenameForDownload } from "./utils/download";
import { WavTagWriter } from "./tagWriters/wavTagWriter";

class TrackError extends Error {
  constructor(message: string, trackId: number) {
    super(`${message} (TrackId: ${trackId})`);
    this.name = "TrackError";
    // Ensures proper stack trace in Node.js
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, TrackError);
    }
  }
}

// Define constants for timeout values, retry attempts, etc.
const CONSTANTS = {
  SEGMENT_DOWNLOAD_TIMEOUT_MS: 30000, // 30 seconds per segment timeout
  ARTWORK_DOWNLOAD_TIMEOUT_MS: 15000, // 15 seconds for artwork download
  MAX_DOWNLOAD_RETRIES: 3,
  RETRY_DELAY_MS: 2000,
  MIME_TYPES: {
    MP3: "audio/mpeg",
    MP4: "audio/mp4",
    WAV: ["audio/x-wav", "audio/wav"]
  },
  FILE_EXTENSIONS: {
    MP3: "mp3",
    M4A: "m4a",
    WAV: "wav"
  }
};

const soundcloudApi = new SoundCloudApi();
const logger = Logger.create("Background");
const manifest = getExtensionManifest();

logger.logInfo("Starting with version: " + manifest.version);

loadConfiguration(true);

interface DownloadData {
  trackId: number;
  title: string;
  duration: number;
  uploadDate: Date;
  username: string;
  userPermalink: string;
  avatarUrl: string;
  artworkUrl: string;
  streamUrl: string;
  fileExtension?: string;
  trackNumber: number | undefined;
  albumName: string | undefined;
  hls: boolean;
}

/**
 * Creates a fetch request with timeout
 */
async function fetchWithTimeout(url: string, options: RequestInit = {}, timeoutMs = 30000): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error ${response.status}: ${response.statusText}`);
    }
    
    return response;
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error(`Request timeout after ${timeoutMs}ms: ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

/**
 * Safely downloads a single HLS segment with retry logic
 */
async function downloadSegment(url: string, retries = CONSTANTS.MAX_DOWNLOAD_RETRIES): Promise<ArrayBuffer> {
  let lastError: Error;
  
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetchWithTimeout(
        url, 
        {}, 
        CONSTANTS.SEGMENT_DOWNLOAD_TIMEOUT_MS
      );
      
      const buffer = await response.arrayBuffer();
      
      // Verify buffer has content
      if (!buffer || buffer.byteLength === 0) {
        throw new Error(`Empty segment received from ${url}`);
      }
      
      return buffer;
    } catch (error) {
      lastError = error;
      logger.logWarn(`Segment download attempt ${attempt + 1}/${retries} failed: ${error.message}`);
      
      // Wait before retry
      if (attempt < retries - 1) {
        await new Promise(resolve => setTimeout(resolve, CONSTANTS.RETRY_DELAY_MS));
      }
    }
  }
  
  throw lastError || new Error(`Failed to download segment after ${retries} attempts`);
}

async function handleDownload(data: DownloadData, reportProgress: (progress?: number) => void) {
  try {
    logger.logInfo(`Initiating download of ${data.trackId} with payload`, { payload: data });

    let artistsString = data.username;
    let titleString = data.title;

    if (getConfigValue("normalize-track")) {
      try {
        const extractor = new MetadataExtractor(data.title, data.username, data.userPermalink);

        let artists = extractor.getArtists();

        if (!getConfigValue("include-producers")) {
          artists = artists.filter((i) => i.type !== ArtistType.Producer);
        }

        artistsString = artists.map((i) => i.name).join(", ");
        titleString = extractor.getTitle();
        const remixers = artists.filter((i) => i.type === ArtistType.Remixer);

        if (remixers.length > 0) {
          const remixerNames = remixers.map((i) => i.name).join(" & ");
          const remixTypeString = RemixType[remixers[0].remixType || RemixType.Remix].toString();

          titleString += ` (${remixerNames} ${remixTypeString})`;
        }
      } catch (error) {
        logger.logError(`Error during metadata extraction for track ${data.trackId}:`, error);
        // Continue with original values if extraction fails
      }
    }

    // Ensure we have valid values
    if (!artistsString) {
      artistsString = "Unknown";
    }

    if (!titleString) {
      titleString = "Unknown";
    }

    const rawFilename = sanitizeFilenameForDownload(`${artistsString} - ${titleString}`);

    let artworkUrl = data.artworkUrl;

    if (!artworkUrl) {
      logger.logInfo(`No Artwork URL could be determined. Fallback to User Avatar (TrackId: ${data.trackId})`);
      artworkUrl = data.avatarUrl;
    }

    logger.logInfo(`Starting download of '${rawFilename}' (TrackId: ${data.trackId})...`);

    let streamBuffer: ArrayBuffer;
    let streamHeaders: Headers;

    if (data.hls) {
      try {
        // Improved HLS download with better error handling
        const playlistReq = await fetchWithTimeout(data.streamUrl, {}, 15000);
        const playlist = await playlistReq.text();

        // @ts-ignore
        const parser = new Parser();

        parser.push(playlist);
        parser.end();

        if (!parser.manifest || !parser.manifest.segments || parser.manifest.segments.length === 0) {
          throw new Error("Invalid or empty HLS playlist");
        }

        const segmentUrls: string[] = parser.manifest.segments.map((i) => i.uri);
        const segments: ArrayBuffer[] = [];
        const failedSegments: { url: string, index: number }[] = [];

        // First pass: download all segments
        for (let i = 0; i < segmentUrls.length; i++) {
          try {
            const segment = await downloadSegment(segmentUrls[i]);
            segments.push(segment);
          } catch (error) {
            // Track failed segment for retry
            failedSegments.push({ url: segmentUrls[i], index: i });
            // Add placeholder to maintain order
            segments.push(null);
            logger.logWarn(`Failed to download segment ${i+1}/${segmentUrls.length}: ${error.message}`);
          }

          // Report progress even if segment failed
          const progress = Math.round(((i + 1) / segmentUrls.length) * 100);
          reportProgress(progress);
        }

        // Retry failed segments
        if (failedSegments.length > 0) {
          logger.logInfo(`Retrying ${failedSegments.length} failed segments for track ${data.trackId}`);
          
          for (const { url, index } of failedSegments) {
            try {
              const segment = await downloadSegment(url, CONSTANTS.MAX_DOWNLOAD_RETRIES);
              segments[index] = segment;
            } catch (error) {
              logger.logError(`Failed to download segment ${index} after multiple retries:`, error);
              throw new Error(`Failed to download segment ${index + 1} of ${segmentUrls.length} after multiple attempts`);
            }
          }
        }

        // Verify all segments were downloaded successfully
        if (segments.some(segment => segment === null)) {
          throw new Error("Some segments failed to download");
        }

        reportProgress(100);
        
        // Concatenate segments into a single buffer
        try {
          streamBuffer = concatArrayBuffers(segments);
          
          // Verify final buffer
          if (!streamBuffer || streamBuffer.byteLength === 0) {
            throw new Error("Concatenated buffer is empty");
          }
          
          // Log buffer size for debugging
          logger.logInfo(`HLS download complete. Total size: ${streamBuffer.byteLength} bytes`);
        } catch (error) {
          logger.logError(`Failed to concatenate HLS segments (TrackId: ${data.trackId}):`, error);
          throw error;
        }
      } catch (error) {
        logger.logError(`Failed to download m3u8 playlist (TrackId: ${data.trackId}):`, error);
        throw error;
      }
    } else {
      try {
        // Download from progressive stream with improved error handling
        [streamBuffer, streamHeaders] = await soundcloudApi.downloadStream(data.streamUrl, reportProgress);
        
        // Verify buffer is valid
        if (!streamBuffer || streamBuffer.byteLength === 0) {
          throw new Error("Received empty stream buffer");
        }
        
        logger.logInfo(`Progressive download complete. Total size: ${streamBuffer.byteLength} bytes`);
      } catch (error) {
        logger.logError(`Failed to download stream (TrackId: ${data.trackId}):`, error);
        throw error;
      }
    }

    if (!streamBuffer) {
      throw new TrackError("Undefined streamBuffer", data.trackId);
    }

    // Determine content type and file extension
    let contentType;
    if (!data.fileExtension && streamHeaders) {
      contentType = streamHeaders.get("content-type");
      let extension = CONSTANTS.FILE_EXTENSIONS.MP3;

      if (contentType === CONSTANTS.MIME_TYPES.MP4) {
        extension = CONSTANTS.FILE_EXTENSIONS.M4A;
      } else if (CONSTANTS.MIME_TYPES.WAV.includes(contentType)) {
        extension = CONSTANTS.FILE_EXTENSIONS.WAV;
      }

      data.fileExtension = extension;

      logger.logInfo(`Inferred file extension from 'content-type' header (TrackId: ${data.trackId})`, {
        contentType,
        extension,
      });
    }

    // Default to original buffer in case metadata setting fails
    let downloadBuffer: ArrayBuffer = streamBuffer;
    let tagWritingSuccessful = false;

    if (getConfigValue("set-metadata")) {
      try {
        let writer: TagWriter;

        if (data.fileExtension === CONSTANTS.FILE_EXTENSIONS.M4A) {
          // Create a copy of the buffer for MP4 processing to prevent corruption
          const bufferCopy = streamBuffer.slice(0);
          
          try {
            const mp4Writer = new Mp4TagWriter(bufferCopy);

            try {
              mp4Writer.setDuration(data.duration);
            } catch (error) {
              logger.logError(`Failed to set duration for track (TrackId: ${data.trackId})`, error);
              // Continue without duration
            }

            writer = mp4Writer;
          } catch (error) {
            logger.logError(`Failed to initialize MP4 tag writer (TrackId: ${data.trackId})`, error);
            throw error;
          }
        } else if (data.fileExtension === CONSTANTS.FILE_EXTENSIONS.MP3) {
          try {
            writer = new Mp3TagWriter(streamBuffer);
          } catch (error) {
            logger.logError(`Failed to initialize MP3 tag writer (TrackId: ${data.trackId})`, error);
            throw error;
          }
        } else if (data.fileExtension === CONSTANTS.FILE_EXTENSIONS.WAV) {
          try {
            writer = new WavTagWriter(streamBuffer);
          } catch (error) {
            logger.logError(`Failed to initialize WAV tag writer (TrackId: ${data.trackId})`, error);
            throw error;
          }
        }

        if (writer) {
          // Set basic metadata
          try {
            writer.setTitle(titleString);
            writer.setAlbum(data.albumName ?? titleString);
            writer.setArtists([artistsString]);
            writer.setComment("https://github.com/NotTobi/soundcloud-dl");

            if (data.trackNumber > 0) {
              writer.setTrackNumber(data.trackNumber);
            }

            const releaseYear = data.uploadDate.getFullYear();
            writer.setYear(releaseYear);
          } catch (error) {
            logger.logError(`Failed to set basic metadata (TrackId: ${data.trackId})`, error);
            // Continue with artwork and buffer generation
          }

          // Download and set artwork with improved error handling
          if (artworkUrl) {
            try {
              const sizeOptions = ["original", "t500x500", "large"];
              let artworkBuffer = null;
              
              // Try each size option until we get a valid artwork
              for (const sizeOption of sizeOptions) {
                if (artworkBuffer !== null) break;
                
                try {
                  const curArtworkUrl = artworkUrl.replace("-large.", `-${sizeOption}.`);
                  artworkBuffer = await soundcloudApi.downloadArtwork(curArtworkUrl);
                  
                  // Verify artwork buffer
                  if (!artworkBuffer || artworkBuffer.byteLength === 0) {
                    logger.logWarn(`Empty artwork buffer received for size ${sizeOption} (TrackId: ${data.trackId})`);
                    artworkBuffer = null;
                    continue;
                  }
                  
                  logger.logInfo(`Successfully downloaded artwork at size ${sizeOption} (TrackId: ${data.trackId})`);
                } catch (error) {
                  logger.logWarn(`Failed to download artwork at size ${sizeOption} (TrackId: ${data.trackId}):`, error);
                  // Try next size
                }
              }

              if (artworkBuffer) {
                try {
                  writer.setArtwork(artworkBuffer);
                } catch (error) {
                  logger.logError(`Failed to set artwork (TrackId: ${data.trackId})`, error);
                  // Continue without artwork
                }
              } else {
                logger.logWarn(`Could not download any artwork (TrackId: ${data.trackId})`);
              }
            } catch (error) {
              logger.logError(`Error processing artwork (TrackId: ${data.trackId})`, error);
              // Continue without artwork
            }
          } else {
            logger.logWarn(`Skipping download of Artwork (TrackId: ${data.trackId})`);
          }

          // Get the final buffer with metadata
          try {
            const processedBuffer = await writer.getBuffer();
            
            // Verify buffer integrity
            if (!processedBuffer || processedBuffer.byteLength === 0) {
              throw new Error("Tag writer returned empty buffer");
            }
            
            // Check if buffer size is reasonable (not corrupted)
            if (processedBuffer.byteLength < streamBuffer.byteLength * 0.5) {
              throw new Error(`Processed buffer is suspiciously small: ${processedBuffer.byteLength} bytes vs original ${streamBuffer.byteLength} bytes`);
            }
            
            downloadBuffer = processedBuffer;
            tagWritingSuccessful = true;
            logger.logInfo(`Successfully processed metadata for track ${data.trackId}`);
          } catch (error) {
            logger.logError(`Failed to get processed buffer (TrackId: ${data.trackId})`, error);
            // Will use original streamBuffer
          }
        }
      } catch (error) {
        logger.logError(`Failed to set metadata (TrackId: ${data.trackId})`, error);
        // Will use original streamBuffer
      }
    }

    // If tag writing was not successful, use the original buffer
    if (!tagWritingSuccessful) {
      logger.logInfo(`Using original stream buffer for track ${data.trackId}`);
      downloadBuffer = streamBuffer;
    }

    // Create blob for download
    const blobOptions: BlobPropertyBag = {};

    if (contentType) blobOptions.type = contentType;

    try {
      const downloadBlob = new Blob([downloadBuffer], blobOptions);
      
      // Verify blob size
      if (downloadBlob.size === 0) {
        throw new Error("Created empty download blob");
      }
      
      logger.logInfo(`Created download blob of size ${downloadBlob.size} bytes`);
      
      const saveAs = !getConfigValue("download-without-prompt");
      const defaultDownloadLocation = getConfigValue("default-download-location");
      let downloadFilename = rawFilename + "." + data.fileExtension;

      if (!saveAs && defaultDownloadLocation) {
        downloadFilename = defaultDownloadLocation + "/" + downloadFilename;
      }

      logger.logInfo(`Downloading track as '${downloadFilename}' (TrackId: ${data.trackId})...`);

      let downloadUrl: string;

      try {
        downloadUrl = URL.createObjectURL(downloadBlob);

        await downloadToFile(downloadUrl, downloadFilename, saveAs);

        logger.logInfo(`Successfully downloaded '${rawFilename}' (TrackId: ${data.trackId})!`);

        reportProgress(101);
      } catch (error) {
        logger.logError(`Failed to download track to file system (TrackId: ${data.trackId})`, {
          downloadFilename,
          saveAs,
          error
        });

        throw new TrackError(`Failed to download track to file system: ${error.message}`, data.trackId);
      } finally {
        if (downloadUrl) URL.revokeObjectURL(downloadUrl);
      }
    } catch (error) {
      logger.logError(`Failed to create blob (TrackId: ${data.trackId})`, error);
      throw error;
    }
  } catch (error) {
    if (error instanceof TrackError) {
      throw error;
    }
    throw new TrackError(`Error during download: ${error.message}`, data.trackId);
  }
}

interface TranscodingDetails {
  url: string;
  protocol: "hls" | "progressive";
  quality: "hq" | "sq";
}

function getTranscodingDetails(details: Track): TranscodingDetails[] | null {
  try {
    if (details?.media?.transcodings?.length < 1) return null;

    const mpegStreams = details.media.transcodings
      .filter(
        (transcoding) =>
          (transcoding.format?.protocol === "progressive" || transcoding.format?.protocol === "hls") &&
          (transcoding.format?.mime_type?.startsWith("audio/mpeg") ||
            transcoding.format?.mime_type?.startsWith("audio/mp4")) &&
          !transcoding.snipped
      )
      .map<TranscodingDetails>((transcoding) => ({
        protocol: transcoding.format.protocol,
        url: transcoding.url,
        quality: transcoding.quality,
      }));

    if (mpegStreams.length < 1) {
      logger.logWarn("No transcodings streams could be determined!");
      return null;
    }

    // prefer 'hq' and 'progressive' streams
    let streams = mpegStreams.sort((a, b) => {
      if (a.quality === "hq" && b.quality === "sq") {
        return -1;
      }

      if (a.quality === "sq" && b.quality === "hq") {
        return 1;
      }

      if (a.protocol === "progressive" && b.protocol === "hls") {
        return -1;
      }

      if (a.protocol === "hls" && b.protocol === "progressive") {
        return 1;
      }

      return 0;
    });

    if (!getConfigValue("download-hq-version")) {
      streams = streams.filter((stream) => stream.quality !== "hq");
    }

    if (streams.some((stream) => stream.quality === "hq")) {
      logger.logInfo("Including high quality streams!");
    }

    return streams;
  } catch (error) {
    logger.logError("Error determining transcoding details:", error);
    return null;
  }
}

// -------------------- HANDLERS --------------------
const authRegex = new RegExp("OAuth (.+)");
const followerIdRegex = new RegExp("/me/followings/(\\d+)");

onBeforeSendHeaders(
  (details) => {
    try {
      let requestHasAuth = false;

      if (details.requestHeaders && getConfigValue("oauth-token") !== null) {
        for (let i = 0; i < details.requestHeaders.length; i++) {
          if (details.requestHeaders[i].name.toLowerCase() !== "authorization") continue;

          requestHasAuth = true;
          const authHeader = details.requestHeaders[i].value;

          const result = authRegex.exec(authHeader);

          if (!result || result.length < 2) continue;

          storeConfigValue("oauth-token", result[1]);
        }

        const oauthToken = getConfigValue("oauth-token");

        if (!requestHasAuth && oauthToken) {
          logger.logDebug("Adding OAuth token to request...", { oauthToken });

          details.requestHeaders.push({
            name: "Authorization",
            value: "OAuth " + oauthToken,
          });

          return {
            requestHeaders: details.requestHeaders,
          };
        }
      }
    } catch (error) {
      logger.logError("Error in onBeforeSendHeaders handler:", error);
    }
  },
  ["*://api-v2.soundcloud.com/*"],
  ["blocking", "requestHeaders"]
);

onBeforeRequest(
  (details) => {
    try {
      const url = new URL(details.url);

      if (url.pathname === "/connect/session" && getConfigValue("oauth-token") === null) {
        logger.logInfo("User logged in");

        storeConfigValue("oauth-token", undefined);
      } else if (url.pathname === "/sign-out") {
        logger.logInfo("User logged out");

        storeConfigValue("oauth-token", null);
        storeConfigValue("user-id", null);
        storeConfigValue("client-id", null);
        storeConfigValue("followed-artists", []);
      } else if (url.pathname.startsWith("/me/followings/")) {
        const followerIdMatch = followerIdRegex.exec(url.pathname);

        if (followerIdMatch && followerIdMatch.length === 2) {
          const followerId = +followerIdMatch[1];

          if (!!followerId) {
            let followedArtists = getConfigValue("followed-artists") || [];

            if (details.method === "POST") {
              followedArtists = [...followedArtists, followerId];
            } else if (details.method === "DELETE") {
              followedArtists = followedArtists.filter((i) => i !== followerId);
            }

            storeConfigValue("followed-artists", followedArtists);
          }
        }
      } else {
        const clientId = url.searchParams.get("client_id");
        const storedClientId = getConfigValue("client-id");

        if (clientId) {
          storeConfigValue("client-id", clientId);
        } else if (storedClientId) {
          logger.logDebug("Adding ClientId to unauthenticated request...", { url, clientId: storedClientId });

          url.searchParams.append("client_id", storedClientId);

          return {
            redirectUrl: url.toString(),
          };
        }
      }
    } catch (error) {
      logger.logError("Error in onBeforeRequest handler:", error);
    }
  },
  ["*://api-v2.soundcloud.com/*", "*://api-auth.soundcloud.com/*"],
  ["blocking"]
);

function isValidTrack(track: Track) {
  return track && track.kind === "track" && track.state === "finished" && (track.streamable || track.downloadable);
}

function isTranscodingDetails(detail: unknown): detail is TranscodingDetails {
  return !!detail && typeof detail === "object" && "protocol" in detail;
}

/**
 * Download track with improved error handling and retry logic
 */
async function downloadTrack(
  track: Track,
  trackNumber: number | undefined,
  albumName: string | undefined,
  reportProgress: (progress?: number) => void
) {
  if (!isValidTrack(track)) {
    logger.logError("Track does not satisfy constraints needed to be downloadable", track);
    throw new TrackError("Track does not satisfy constraints needed to be downloadable", track.id);
  }

  const downloadDetails: Array<StreamDetails | TranscodingDetails> = [];
  const errors: Error[] = []; // Collect errors for better reporting

  // Try to get original version if available
  if (getConfigValue("download-original-version") && track.downloadable && track.has_downloads_left) {
    try {
      const originalDownloadUrl = await soundcloudApi.getOriginalDownloadUrl(track.id);

      if (originalDownloadUrl) {
        const stream: StreamDetails = {
          url: originalDownloadUrl,
          hls: false,
        };

        downloadDetails.push(stream);
        logger.logInfo(`Added original download option for track ${track.id}`);
      }
    } catch (error) {
      errors.push(error);
      logger.logWarn(`Failed to get original download URL for track ${track.id}:`, error);
      // Continue with transcoding options
    }
  }

  const transcodingDetails = getTranscodingDetails(track);

  if (transcodingDetails) {
    downloadDetails.push(...transcodingDetails);
    logger.logInfo(`Added ${transcodingDetails.length} transcoding options for track ${track.id}`);
  }

  if (downloadDetails.length < 1) {
    throw new TrackError(`No download details could be determined: ${errors.map(e => e.message).join('; ')}`, track.id);
  }

  // Try each download option in order until one succeeds
  for (const downloadDetail of downloadDetails) {
    let stream: StreamDetails;

    try {
      if (isTranscodingDetails(downloadDetail)) {
        logger.logDebug(`Trying ${downloadDetail.protocol}/${downloadDetail.quality} stream for track ${track.id}`);
        stream = await soundcloudApi.getStreamDetails(downloadDetail.url);
      } else {
        logger.logDebug(`Trying original download stream for track ${track.id}`);
        stream = downloadDetail;
      }

      const downloadData: DownloadData = {
        trackId: track.id,
        duration: track.duration,
        uploadDate: new Date(track.display_date),
        streamUrl: stream.url,
        fileExtension: stream.extension,
        title: track.title,
        username: track.user.username,
        userPermalink: track.user.permalink,
        artworkUrl: track.artwork_url,
        avatarUrl: track.user.avatar_url,
        trackNumber,
        albumName,
        hls: stream.hls,
      };

      await handleDownload(downloadData, reportProgress);
      
      // If we got here, download was successful
      return;
    } catch (error) {
      // Log error and continue with next option
      errors.push(error);
      
      const detailType = isTranscodingDetails(downloadDetail) 
        ? `${downloadDetail.protocol}/${downloadDetail.quality}` 
        : 'original';
        
      logger.logWarn(`Failed to download track ${track.id} using ${detailType} option:`, error);
      // Continue with next option
    }
  }

  // If we get here, all options failed
  const errorMessages = errors.map(e => e.message).join('; ');
  throw new TrackError(`No version of this track could be downloaded: ${errorMessages}`, track.id);
}

interface Playlist {
  tracks: Track[];
  set_type: string;
  title: string;
}

interface DownloadRequest {
  type: string;
  url: string;
  downloadId: string;
  isRetry?: boolean;
  retryCount?: number;
}

interface DownloadProgress {
  downloadId: string;
  progress?: number;
  error?: string;
  status?: string;
}

// Track active downloads to allow cancellation
const activeDownloads: Record<string, { aborted: boolean, trackId: number }> = {};

function sendDownloadProgress(tabId: number, downloadId: string, progress?: number, error?: Error | string, status?: string) {
  try {
    let errorMessage: string = "";

    if (error instanceof Error) {
      errorMessage = error.message;
    } else if (typeof error === 'string') {
      errorMessage = error;
    }

    const downloadProgress: DownloadProgress = {
      downloadId,
      progress,
      error: errorMessage,
      status
    };

    sendMessageToTab(tabId, downloadProgress);
  } catch (err) {
    logger.logError("Error sending download progress:", err);
  }
}

function chunkArray<T>(array: T[], chunkSize: number) {
  if (chunkSize < 1) throw new Error("Invalid chunk size");

  const chunks: T[][] = [];

  for (let i = 0; i < array.length; i += chunkSize) {
    const chunk = array.slice(i, i + chunkSize);
    chunks.push(chunk);
  }

  return chunks;
}

onMessage(async (sender, message: DownloadRequest) => {
  const tabId = sender.tab?.id;
  const { downloadId, url, type, isRetry, retryCount = 0 } = message;

  if (!tabId) {
    logger.logError("Message received with invalid tab ID");
    return;
  }

  // Register this download
  activeDownloads[downloadId] = { 
    aborted: false,
    trackId: 0 // Will be updated when we resolve the track
  };

  try {
    if (type === "DOWNLOAD_SET") {
      logger.logDebug("Received set download request", { url });
      sendDownloadProgress(tabId, downloadId, 0, undefined, "preparing");

      try {
        const set = await soundcloudApi.resolveUrl<Playlist>(url);
        if (!set || !set.tracks || set.tracks.length === 0) {
          throw new Error("Could not resolve set or set contains no tracks");
        }
        
        const isAlbum = set.set_type === "album" || set.set_type === "ep";
        const trackIds = set.tracks.map((i) => i.id);

        // Progress tracking per track
        const progresses: { [key: number]: number } = {};
        trackIds.forEach(id => { progresses[id] = 0 });

        const reportPlaylistProgress = (trackId: number) => (progress?: number) => {
          // Don't update if download was aborted
          if (activeDownloads[downloadId]?.aborted) return;
          
          if (progress) {
            progresses[trackId] = progress;
          }

          const totalProgress = Object.values(progresses).reduce((acc, cur) => acc + cur, 0);
          const avgProgress = totalProgress / trackIds.length;

          sendDownloadProgress(
            tabId, 
            downloadId, 
            Math.floor(avgProgress),
            undefined, 
            avgProgress < 100 ? "downloading" : 
              avgProgress === 100 ? "finishing" : "completed"
          );
        };

        const treatAsAlbum = isAlbum && trackIds.length > 1;
        const albumName = treatAsAlbum ? set.title : undefined;

        // Process tracks in smaller chunks to avoid overwhelming the system
        const trackIdChunkSize = 5; // Reduced from 10 for better stability
        const trackIdChunks = chunkArray(trackIds, trackIdChunkSize);

        logger.logInfo(`Downloading ${isAlbum ? "album" : "playlist"} with ${trackIds.length} tracks in ${trackIdChunks.length} chunks`);

        let currentTrackIdChunk = 0;
        let successCount = 0;
        
        for (const trackIdChunk of trackIdChunks) {
          // Check if download was aborted
          if (activeDownloads[downloadId]?.aborted) {
            logger.logInfo(`Download ${downloadId} was aborted before processing chunk ${currentTrackIdChunk + 1}`);
            break;
          }
          
          const baseTrackNumber = currentTrackIdChunk * trackIdChunkSize;

          try {
            const keyedTracks = await soundcloudApi.getTracks(trackIdChunk);
            
            if (!keyedTracks || Object.keys(keyedTracks).length === 0) {
              logger.logWarn(`No tracks returned for chunk ${currentTrackIdChunk + 1}`);
              currentTrackIdChunk++;
              continue;
            }
            
            const tracks = Object.values(keyedTracks).filter(Boolean);
            
            // Process tracks sequentially to avoid race conditions
            for (let i = 0; i < tracks.length; i++) {
              // Check if download was aborted
              if (activeDownloads[downloadId]?.aborted) {
                logger.logInfo(`Download ${downloadId} was aborted during track processing`);
                break;
              }
              
              const track = tracks[i];
              if (!track) continue;
              
              // Update current track ID in active downloads
              activeDownloads[downloadId].trackId = track.id;
              
              const trackNumber = treatAsAlbum ? baseTrackNumber + i + 1 : undefined;
              
              try {
                await downloadTrack(
                  track, 
                  trackNumber, 
                  albumName, 
                  reportPlaylistProgress(track.id)
                );
                
                successCount++;
              } catch (error) {
                logger.logError(`Failed to download track ${track.id} in set:`, error);
                // Update progress to show failure
                progresses[track.id] = 0;
                // Continue with next track
              }
            }
          } catch (error) {
            logger.logError(`Error processing chunk ${currentTrackIdChunk + 1}:`, error);
            // Continue with next chunk
          }
          
          currentTrackIdChunk++;
        }
        
        // Check success rate and report completion
        if (successCount === 0) {
          throw new Error("Failed to download any tracks from the set");
        }
        
        logger.logInfo(`Downloaded ${successCount}/${trackIds.length} tracks from ${isAlbum ? "album" : "playlist"}`);
        
        // Final progress update
        sendDownloadProgress(tabId, downloadId, 101, undefined, "completed");
      } catch (error) {
        logger.logError(`Error downloading set:`, error);
        sendDownloadProgress(tabId, downloadId, undefined, error, "error");
      }
    } else if (type === "DOWNLOAD") {
      logger.logDebug("Received track download request", { url, isRetry });
      sendDownloadProgress(tabId, downloadId, 0, undefined, "preparing");

      try {
        const track = await soundcloudApi.resolveUrl<Track>(url);
        
        if (!track) {
          throw new Error("Could not resolve track URL");
        }
        
        // Update track ID in active downloads
        activeDownloads[downloadId].trackId = track.id;

        const reportTrackProgress = (progress?: number) => {
          // Don't update if download was aborted
          if (activeDownloads[downloadId]?.aborted) return;
          
          let status = "downloading";
          
          if (progress === 100) status = "finishing";
          else if (progress === 101) status = "completed";
          
          sendDownloadProgress(tabId, downloadId, progress, undefined, status);
        };

        await downloadTrack(track, undefined, undefined, reportTrackProgress);
        
        // Success
        logger.logInfo(`Successfully downloaded track ${track.id}`);
      } catch (error) {
        logger.logError(`Error downloading track:`, error);
        sendDownloadProgress(tabId, downloadId, undefined, error, "error");
      }
    } else {
      logger.logError(`Unknown download type: ${type}`);
      sendDownloadProgress(tabId, downloadId, undefined, `Unknown download type: ${type}`, "error");
    }
  } catch (error) {
    logger.logError(`Unexpected error in message handler:`, error);
    sendDownloadProgress(tabId, downloadId, undefined, error, "error");
  } finally {
    // Clean up active download
    delete activeDownloads[downloadId];
  }
});

onPageActionClicked(() => {
  openOptionsPage();
});

const oauthTokenChanged = async (token: string) => {
  if (!token) return;

  try {
    const user = await soundcloudApi.getCurrentUser();

    if (!user) {
      logger.logError("Failed to fetch currently logged in user");
      return;
    }

    storeConfigValue("user-id", user.id);
    logger.logInfo("Logged in as", user.username);

    try {
      const followedArtistIds = await soundcloudApi.getFollowedArtistIds(user.id);

      if (!followedArtistIds) {
        logger.logWarn("Failed to fetch ids of followed artists");
        return;
      }

      storeConfigValue("followed-artists", followedArtistIds);
    } catch (error) {
      logger.logError("Error fetching followed artists:", error);
    }
  } catch (error) {
    logger.logError("Error processing OAuth token change:", error);
  }
};

registerConfigChangeHandler("oauth-token", oauthTokenChanged);

// Add global error handler
window.addEventListener('error', (event) => {
  logger.logError("Unhandled error in background script:", event.error || event.message);
});

// Add unhandled promise rejection handler
window.addEventListener('unhandledrejection', (event) => {
  logger.logError("Unhandled promise rejection in background script:", event.reason);
});