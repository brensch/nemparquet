package util

import (
	"strings"

	"golang.org/x/net/html"
)

// ParseLinks finds links ending with a specific suffix within an HTML node tree.
// It performs a depth-first search for <a> tags and checks their href attribute.
func ParseLinks(n *html.Node, suffix string) []string {
	var out []string
	var walk func(*html.Node)

	walk = func(nd *html.Node) {
		// Check if it's an <a> element
		if nd.Type == html.ElementNode && nd.Data == "a" {
			// Iterate through attributes to find 'href'
			for _, a := range nd.Attr {
				if a.Key == "href" {
					// Check if the href value ends with the desired suffix (case-insensitive)
					// Also ensure it's not just a link to the root directory "/"
					if strings.HasSuffix(strings.ToLower(a.Val), strings.ToLower(suffix)) && a.Val != "/" {
						out = append(out, a.Val) // Add the link value to the output slice
					}
					// No need to check other attributes for this node once href is found
					break
				}
			}
		}
		// Recursively walk through child nodes
		for c := nd.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}

	// Start the walk from the root node provided
	walk(n)
	return out
}
