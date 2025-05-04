package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"sort"

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/downloader"
)

// DiscoverArchiveURLs discovers all unique archive ZIP URLs from the feeds specified in the config.
// It returns a sorted slice of the discovered URLs.
func DiscoverArchiveURLs(ctx context.Context, cfg config.Config, logger *slog.Logger) ([]string, error) {
	logger.Info("Discovering archive ZIP URLs...")

	// Discover URLs from the configured archive feed URLs
	archiveURLsMap, archiveDiscoveryErr := downloader.DiscoverZipURLs(ctx, cfg.ArchiveFeedURLs, logger)
	if archiveDiscoveryErr != nil {
		// Log the error but potentially continue if some URLs were found
		logger.Error("Errors occurred during archive discovery phase.", "error", archiveDiscoveryErr)
	}

	// Check if the context was cancelled during discovery
	if ctx.Err() != nil {
		logger.Warn("Discovery cancelled by context.", "error", ctx.Err())
		// Combine potential discovery error with context error
		return nil, errors.Join(archiveDiscoveryErr, ctx.Err())
	}

	// Extract URLs into a slice
	discoveredArchiveURLs := make([]string, 0, len(archiveURLsMap))
	for url := range archiveURLsMap {
		discoveredArchiveURLs = append(discoveredArchiveURLs, url)
	}

	// Check if any URLs were found
	if len(discoveredArchiveURLs) == 0 {
		logger.Info("No archive zip URLs discovered.")
		// Return the discovery error if one occurred, otherwise indicate none found
		if archiveDiscoveryErr != nil {
			return nil, archiveDiscoveryErr
		}
		return nil, errors.New("no archive zip URLs discovered")
	}

	// Sort the URLs for consistent processing order
	sort.Strings(discoveredArchiveURLs)

	logger.Info("Archive discovery complete.", slog.Int("total_archive_zips", len(discoveredArchiveURLs)))

	// Return the discovered URLs and any non-fatal discovery error that might have occurred
	return discoveredArchiveURLs, archiveDiscoveryErr
}
