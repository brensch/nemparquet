// main.go
package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/net/html"
)

// Directories hosting the CSV feeds on NEMWEB
var feedURLs = []string{
	"https://nemweb.com.au/Reports/Current/FPP/",
	"https://nemweb.com.au/Reports/Current/FPPDAILY/",
	"https://nemweb.com.au/Reports/Current/FPPRATES/",
	"https://nemweb.com.au/Reports/Current/FPPRUN/",
	"https://nemweb.com.au/Reports/Current/PD7Day/",
	"https://nemweb.com.au/Reports/Current/P5_Reports/",
}

func main() {
	inputDir := flag.String("input", "./input_csv", "Directory to download & extract latest CSVs into")
	outputDir := flag.String("output", "./output_parquet", "Directory to write Parquet files to")
	flag.Parse()

	// Ensure directories exist
	for _, d := range []string{*inputDir, *outputDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			log.Fatalf("mkdir %s: %v", d, err)
		}
	}

	// Download and extract latest CSVs
	if err := downloadAndExtractLatest(*inputDir); err != nil {
		log.Fatalf("download/extract: %v", err)
	}

	// Process each CSV
	csvFiles, err := filepath.Glob(filepath.Join(*inputDir, "*.CSV"))
	if err != nil {
		log.Fatalf("glob CSVs: %v", err)
	}
	for _, csvPath := range csvFiles {
		if err := processCSVSections(csvPath, *outputDir); err != nil {
			log.Printf("ERROR processing %s: %v", csvPath, err)
		}
	}
}

// downloadAndExtractLatest fetches each feed URL, identifies the latest .zip,
// downloads it, and extracts its .CSV files into dir.
func downloadAndExtractLatest(dir string) error {
	client := &http.Client{}
	for _, baseURL := range feedURLs {
		log.Printf("Listing %s …", baseURL)
		resp, err := client.Get(baseURL)
		if err != nil {
			return fmt.Errorf("GET %s: %w", baseURL, err)
		}
		root, err := html.Parse(resp.Body)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("parse HTML %s: %w", baseURL, err)
		}

		base, err := url.Parse(baseURL)
		if err != nil {
			return fmt.Errorf("parse base URL %s: %w", baseURL, err)
		}

		links := parseLinks(root, ".zip")
		if len(links) == 0 {
			log.Printf(" No ZIP files at %s", baseURL)
			continue
		}
		sort.Strings(links)
		latest := links[len(links)-1]
		zipURL := base.ResolveReference(&url.URL{Path: latest}).String()
		log.Printf(" Downloading %s", zipURL)

		data, err := downloadFile(client, zipURL)
		if err != nil {
			log.Printf("skip %s: %v", zipURL, err)
			continue
		}
		zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			log.Printf("invalid zip %s: %v", zipURL, err)
			continue
		}
		for _, f := range zr.File {
			if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
				in, err := f.Open()
				if err != nil {
					return err
				}
				outPath := filepath.Join(dir, filepath.Base(f.Name))
				out, err := os.Create(outPath)
				if err != nil {
					in.Close()
					return err
				}
				if _, err := io.Copy(out, in); err != nil {
					in.Close()
					out.Close()
					return err
				}
				in.Close()
				out.Close()
				log.Printf("  Extracted %s", outPath)
			}
		}
	}
	return nil
}

// processCSVSections reads the CSV via bufio, splits into sections by "I" records,
// and writes each section to Parquet using Xitongsys CSVWriter.
func processCSVSections(csvPath, outDir string) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open CSV: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	var pw *writer.CSVWriter
	var fw source.ParquetFile
	baseName := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))

	for scanner.Scan() {
		rec := strings.Split(scanner.Text(), ",")
		if len(rec) == 0 {
			continue
		}
		if rec[0] == "I" {
			// Close previous section
			if pw != nil {
				pw.WriteStop()
				fw.Close()
			}
			comp, ver := rec[2], rec[3]
			headers := rec[4:]

			// Peek next line for type inference
			var firstData []string
			if scanner.Scan() {
				nxt := strings.Split(scanner.Text(), ",")
				if len(nxt) >= len(headers)+4 && nxt[0] == "D" {
					firstData = nxt[4:]
				}
			}

			// Build metadata entries in 'key=value' format
			meta := make([]string, len(headers))
			for i := range headers {
				var typ string
				if len(firstData) > i {
					val := strings.Trim(firstData[i], `"`)
					if val == "TRUE" || val == "FALSE" {
						typ = "BOOLEAN"
					} else if _, err := strconv.ParseInt(val, 10, 32); err == nil {
						typ = "INT32"
					} else if _, err := strconv.ParseInt(val, 10, 64); err == nil {
						typ = "INT64"
					} else if _, err := strconv.ParseFloat(val, 64); err == nil {
						typ = "DOUBLE"
					} else {
						typ = "BYTE_ARRAY"
					}
				} else {
					typ = "BYTE_ARRAY"
				}
				if typ == "BYTE_ARRAY" {
					meta[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY", headers[i])
				} else {
					meta[i] = fmt.Sprintf("name=%s, type=%s", headers[i], typ)
				}
			}

			// Create Parquet writer
			parquetFile := fmt.Sprintf("%s_%s_v%s.parquet", baseName, comp, ver)
			path := filepath.Join(outDir, parquetFile)
			fw, err = local.NewLocalFileWriter(path)
			if err != nil {
				return fmt.Errorf("create parquet: %w", err)
			}
			pw, err = writer.NewCSVWriter(meta, fw, 4)
			if err != nil {
				return fmt.Errorf("init writer: %w", err)
			}
			pw.CompressionType = parquet.CompressionCodec_SNAPPY
			log.Printf("Writing section %s v%s to %s", comp, ver, path)

			// Write the first data row if available
			if len(firstData) > 0 {
				if err := pw.Write(firstData); err != nil {
					return fmt.Errorf("write first row: %w", err)
				}
			}

		} else if rec[0] == "D" {
			if pw == nil {
				continue
			}
			values := rec[4:]
			if err := pw.Write(values); err != nil {
				return fmt.Errorf("write row: %w", err)
			}
		}
	}
	// Finalize last section
	if pw != nil {
		pw.WriteStop()
		fw.Close()
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan CSV: %w", err)
	}
	return nil
}

// parseLinks returns href attribute values ending with suffix
func parseLinks(n *html.Node, suffix string) []string {
	var out []string
	var walk func(*html.Node)
	walk = func(nd *html.Node) {
		if nd.Type == html.ElementNode && nd.Data == "a" {
			for _, a := range nd.Attr {
				if a.Key == "href" && strings.HasSuffix(strings.ToLower(a.Val), suffix) {
					out = append(out, a.Val)
				}
			}
		}
		for c := nd.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return out
}

// downloadFile GETs URL and returns body bytes
func downloadFile(client *http.Client, url string) ([]byte, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
