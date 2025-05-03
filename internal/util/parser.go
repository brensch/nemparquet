package util

import (
	"strings"

	"golang.org/x/net/html"
)

// ParseLinks finds links with a specific suffix in an HTML node.
func ParseLinks(n *html.Node, suffix string) []string {
	var out []string
	var walk func(*html.Node)
	walk = func(nd *html.Node) {
		if nd.Type == html.ElementNode && nd.Data == "a" {
			for _, a := range nd.Attr {
				if a.Key == "href" {
					if strings.HasSuffix(strings.ToLower(a.Val), strings.ToLower(suffix)) && a.Val != "/" {
						out = append(out, a.Val)
					}
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
