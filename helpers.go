package tcpsrv

import (
	"fmt"
	"os"
	"strings"
)

//trying to get missing changes...

// GetEnv gets the environment variable if the key is found else returns the fallback value
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// PrintEnv prints the platforms env vars
func PrintEnv() {
	// fetches all env variables
	for _, element := range os.Environ() {
		variable := strings.Split(element, "=")
		fmt.Println(variable[0], "=>", variable[1])
	}
}
