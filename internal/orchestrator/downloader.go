package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/brensch/nemparquet/internal/config" // Needed for source feed approx
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/downloader" // For user agent
	"github.com/brensch/nemparquet/internal/util"
)

// DownloadArchive downloads the content of a given archive URL.
// It logs download events to the database.
// Returns the downloaded data as a byte slice and any error encountered.
func DownloadArchive(ctx context.Context, cfg config.Config, dbConnPool *sql.DB, logger *slog.Logger, client *http.Client, archiveURL string) ([]byte, error) {
	l := logger.With(slog.String("archive_url", archiveURL))
	l.Info("Starting archive download.")

	startTime := time.Now()
	sourceFeed := "" // Determine source feed if possible for logging
	if len(cfg.ArchiveFeedURLs) > 0 {
		sourceFeed = cfg.ArchiveFeedURLs[0] // Approximation
	}
	// Log download start event
	db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventDownloadStart, sourceFeed, "", "", "", nil)

	req, err := http.NewRequestWithContext(ctx, "GET", archiveURL, nil)
	if err != nil {
		errMsg := fmt.Sprintf("failed create request for archive %s: %v", archiveURL, err)
		l.Error(errMsg)
		db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventError, sourceFeed, "", errMsg, "", nil)
		return nil, fmt.Errorf(errMsg)
	}
	req.Header.Set("User-Agent", downloader.GetRandomUserAgent())
	req.Header.Set("Accept", "*/*")

	var downloadedData []byte
	var downloadErr error
	progressCallback := func(dlBytes int64, totalBytes int64) {
		// Log progress minimally
		if totalBytes > 0 {
			l.Debug("Download progress", slog.Int64("dl_mb", dlBytes>>20), slog.Int64("tot_mb", totalBytes>>20), slog.Float64("pct", float64(dlBytes)*100.0/float64(totalBytes)))
		} else {
			l.Debug("Download progress", slog.Int64("dl_mb", dlBytes>>20))
		}
	}
	downloadedData, downloadErr = util.DownloadFileWithProgress(client, req, progressCallback)
	downloadDuration := time.Since(startTime)

	if downloadErr != nil {
		dbLogMessage := fmt.Sprintf("download failed: %v", downloadErr)
		if errors.Is(downloadErr, context.Canceled) || errors.Is(downloadErr, context.DeadlineExceeded) {
			l.Warn("Download cancelled.", "error", downloadErr)
			dbLogMessage = fmt.Sprintf("download cancelled: %v", downloadErr)
		} else {
			l.Error("Download failed.", "error", downloadErr)
		}
		// Log download error event
		db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventError, sourceFeed, "", dbLogMessage, "", &downloadDuration)
		return nil, fmt.Errorf("download %s: %w", archiveURL, downloadErr) // Return download error
	}

	l.Info("Download complete.", slog.Int("bytes", len(downloadedData)), slog.Duration("duration", downloadDuration.Round(time.Millisecond)))
	// Log successful download event
	db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventDownloadEnd, sourceFeed, "", "", "", &downloadDuration)

	return downloadedData, nil
}
