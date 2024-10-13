package selfcert

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	cryrand "crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/crypto/argon2"
	"golang.org/x/term"
)

// EncryptionParameters holds the Argon2id parameters used for key derivation.
type EncryptionParameters struct {
	Time        uint32 // Number of iterations
	Memory      uint32 // Memory usage in KB
	Threads     uint8  // Degree of parallelism
	KeyLength   uint32 // Length of the derived key in bytes
	Salt        []byte // Random salt
	Nonce       []byte // Nonce used in AES-GCM
	CipherSuite string // Cipher suite used (e.g., AES-GCM)
}

// DefaultEncryptionParameters provides default settings for Argon2id and encryption.
var DefaultEncryptionParameters = EncryptionParameters{
	Time:        2,           // Iterations
	Memory:      1024 * 1024, // 1 GB
	Threads:     1,           // Parallelism
	KeyLength:   32,          // 256-bit key for AES-256
	CipherSuite: "AES-GCM",
}

// encryptPrivateKey encrypts the private key using Argon2id and AES-GCM, then encodes it in PEM format.
// It stores all encryption parameters in the PEM headers.
func encryptPrivateKey(privateKey []byte, password []byte, params *EncryptionParameters) (encryptedPEM []byte, err error) {
	if params == nil {
		// Use default parameters if none are provided
		params = &DefaultEncryptionParameters
	}

	// Generate a random salt
	params.Salt = make([]byte, 16) // 128-bit salt
	if _, err := cryrand.Read(params.Salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	// Derive a key using Argon2id
	key := argon2.IDKey(password, params.Salt, params.Time, params.Memory, params.Threads, params.KeyLength)

	// Create AES-GCM cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Generate a random nonce
	params.Nonce = make([]byte, aesGCM.NonceSize())
	if _, err := cryrand.Read(params.Nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt the private key
	ciphertext := aesGCM.Seal(nil, params.Nonce, privateKey, nil)

	// Combine nonce and ciphertext
	encryptedData := append(params.Nonce, ciphertext...)

	// Create PEM headers with encryption parameters
	headers := map[string]string{
		"CipherSuite":        strconv.Quote(params.CipherSuite),
		"Argon2id.Time":      strconv.FormatUint(uint64(params.Time), 10),
		"Argon2id.Memory":    strconv.FormatUint(uint64(params.Memory), 10),
		"Argon2id.Threads":   strconv.FormatUint(uint64(params.Threads), 10),
		"Argon2id.KeyLength": strconv.FormatUint(uint64(params.KeyLength), 10),
		"Argon2id.Salt":      hex.EncodeToString(params.Salt),
		"AESGCM.Nonce":       hex.EncodeToString(params.Nonce),
	}

	// Create a PEM block with the encrypted data and headers
	pemBlock := &pem.Block{
		Type:    "ENCRYPTED PRIVATE KEY",
		Headers: headers,
		Bytes:   encryptedData,
	}

	return pem.EncodeToMemory(pemBlock), nil
}

// decryptPrivateKey decrypts the PEM-encoded encrypted private key using stored encryption parameters.
func decryptPrivateKey(encryptedPEM []byte, password []byte) (ed25519.PrivateKey, error) {
	// Decode the PEM block
	pemBlock, _ := pem.Decode(encryptedPEM)
	if pemBlock == nil {
		return nil, errors.New("failed to decode PEM block")
	}

	// Check PEM type
	if pemBlock.Type != "ENCRYPTED PRIVATE KEY" {
		return nil, fmt.Errorf("unexpected PEM type: %s", pemBlock.Type)
	}

	// Extract and parse encryption parameters from headers
	params, err := parseEncryptionParameters(pemBlock.Headers)
	if err != nil {
		return nil, fmt.Errorf("failed to parse encryption parameters: %w", err)
	}

	// Derive the key using Argon2id with extracted parameters
	key := argon2.IDKey(password, params.Salt, params.Time, params.Memory, params.Threads, params.KeyLength)

	// Ensure the CipherSuite matches expected value
	if params.CipherSuite != "AES-GCM" {
		return nil, fmt.Errorf("unsupported cipher suite (only AES-GCM accepted): '%v'", params.CipherSuite)
	}

	// Create AES-GCM cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Ensure the encrypted data is at least as long as the nonce
	if len(pemBlock.Bytes) < aesGCM.NonceSize() {
		return nil, errors.New("ciphertext too short")
	}

	// Split nonce and ciphertext
	nonce := pemBlock.Bytes[:aesGCM.NonceSize()]
	ciphertext := pemBlock.Bytes[aesGCM.NonceSize():]

	// Decrypt the ciphertext
	decryptedData, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data: %w", err)
	}
	fmt.Printf("decrypt good.\n")
	//fmt.Printf("%v\n", string(decryptedData))

	// Parse the private key (Ed25519)
	privkey, err := x509.ParsePKCS8PrivateKey(decryptedData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	// Assert the key type to Ed25519
	edKey, ok := privkey.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("not an Ed25519 private key")
	}

	return edKey, nil
}

// parseEncryptionParameters extracts and parses encryption parameters from PEM headers.
func parseEncryptionParameters(headers map[string]string) (*EncryptionParameters, error) {
	params := &EncryptionParameters{}

	// Helper function to unquote and parse string values
	unquote := func(s string) string {
		if len(s) > 0 && s[0] == '"' && s[len(s)-1] == '"' {
			return s[1 : len(s)-1]
		}
		return s
	}

	// Parse CipherSuite
	if cipherSuite, ok := headers["CipherSuite"]; ok {
		params.CipherSuite = unquote(cipherSuite)
	} else {
		return nil, errors.New("missing CipherSuite header")
	}

	// Parse Argon2id.Time
	if timeStr, ok := headers["Argon2id.Time"]; ok {
		timeVal, err := strconv.ParseUint(timeStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid Argon2id.Time: %w", err)
		}
		params.Time = uint32(timeVal)
	} else {
		return nil, errors.New("missing Argon2id.Time header")
	}

	// Parse Argon2id.Memory
	if memoryStr, ok := headers["Argon2id.Memory"]; ok {
		memoryVal, err := strconv.ParseUint(memoryStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid Argon2id.Memory: %w", err)
		}
		params.Memory = uint32(memoryVal)
	} else {
		return nil, errors.New("missing Argon2id.Memory header")
	}

	// Parse Argon2id.Threads
	if threadsStr, ok := headers["Argon2id.Threads"]; ok {
		threadsVal, err := strconv.ParseUint(threadsStr, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid Argon2id.Threads: %w", err)
		}
		params.Threads = uint8(threadsVal)
	} else {
		return nil, errors.New("missing Argon2id.Threads header")
	}

	// Parse Argon2id.KeyLength
	if keyLengthStr, ok := headers["Argon2id.KeyLength"]; ok {
		keyLengthVal, err := strconv.ParseUint(keyLengthStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid Argon2id.KeyLength: %w", err)
		}
		params.KeyLength = uint32(keyLengthVal)
	} else {
		return nil, errors.New("missing Argon2id.KeyLength header")
	}

	// Parse Argon2id.Salt
	if saltHex, ok := headers["Argon2id.Salt"]; ok {
		salt, err := hex.DecodeString(saltHex)
		if err != nil {
			return nil, fmt.Errorf("invalid Argon2id.Salt: %w", err)
		}
		params.Salt = salt
	} else {
		return nil, errors.New("missing Argon2id.Salt header")
	}

	// Parse AESGCM.Nonce
	if nonceHex, ok := headers["AESGCM.Nonce"]; ok {
		nonce, err := hex.DecodeString(nonceHex)
		if err != nil {
			return nil, fmt.Errorf("invalid AESGCM.Nonce: %w", err)
		}
		params.Nonce = nonce
	} else {
		return nil, errors.New("missing AESGCM.Nonce header")
	}

	return params, nil
}

func SavePrivateKeyToPathUnderPassphrase(privateKey []byte, path string) error {

	password, err := getPasswordFromTerminal(true)
	if err != nil {
		return fmt.Errorf("could not get pw from terminal: '%v'", err)
	}

	// // these take about 1.5 on rog to encode.
	customParams := &EncryptionParameters{
		Time:        2,           // More iterations for increased security, t.
		Memory:      1024 * 1024, // 1 GB
		Threads:     1,           // Increased parallelism, p.
		KeyLength:   32,          // 256-bit key
		CipherSuite: "AES-GCM",
	}

	// Encrypt the private key
	t0 := time.Now()
	encryptedPEM, err := encryptPrivateKey(privateKey, password, customParams)
	if err != nil {
		return fmt.Errorf("Encryption failed: %v\n", err)
	}

	fmt.Printf("Encrypted Private Key in %v\n", time.Since(t0))
	fmt.Println(string(encryptedPEM))
	fd, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("Could not create key path '%v': %v\n", path, err)
	}
	err = writeAll(fd, encryptedPEM)
	if err != nil {
		fd.Close()
		return fmt.Errorf("Could not write all of key to path '%v': %v\n", path, err)
	}
	fd.Close()
	return nil
}

// asks for password
func LoadEncryptedEd25519PrivateKey(path string) (decryptedPrivateKey []byte, err error) {

	encryptedPEM2, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read encrypted private key file '%v': '%v'", path, err)
	}

	password2, err := getPasswordFromTerminal(false)
	if err != nil {
		return nil, fmt.Errorf("could not get password from terminal: '%v'", err)
	}
	fmt.Printf("trying to unlock and load the private key...\n")

	// Decrypt the private key
	//t1 := time.Now()
	decryptedKey, err := decryptPrivateKey(encryptedPEM2, password2)
	if err != nil {
		return nil, fmt.Errorf("Decryption of path '%v' with supplied pw failed: %v", path, err)
	}

	//fmt.Printf("Decrypted Private Key: in %v\n", time.Since(t1))
	//fmt.Println(string(decryptedKey))
	return decryptedKey, nil
}

// writeAll writes the entire byte slice to the provided file descriptor (fd).
// It will keep retrying until all bytes are written or an error occurs.
func writeAll(fd *os.File, data []byte) error {
	totalWritten := 0
	for {
		// Attempt to write the remaining bytes
		n, err := fd.Write(data[totalWritten:])
		totalWritten += n
		if totalWritten == len(data) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// get passwords from terminal

func getPasswordFromTerminal(confirm bool) ([]byte, error) {
	// First password input

	password1, err := readPassword("Enter passphrase (empty for no passphrase): ")
	//password1, err := readPasswordWithMask("Enter password: ", '🔑')
	if err != nil {
		return nil, fmt.Errorf("Failed to read password: '%v'", err)
	}

	if confirm {
		// Second password input to confirm
		password2, err := readPassword("Enter same passphrase again: ")
		//password2, err := readPasswordWithMask("Confirm password: ", '🔑')
		if err != nil {
			return nil, fmt.Errorf("Failed to read password: '%v'", err)
		}

		// Check if both passwords match
		if !bytes.Equal(password1, password2) {
			return nil, fmt.Errorf("Passwords do not match. Please try again.")
		}
		fmt.Println("Passwords match. Proceeding...")
	}

	return password1, nil
}

// readPassword prompts the user for a password without echoing it
func readPassword(prompt string) ([]byte, error) {
	fmt.Print(prompt)
	passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return nil, err
	}
	fmt.Println() // move to a new line after input
	return passwordBytes, nil
}

// try again without showing pw
/*
// readPasswordWithMask prompts for a password, showing a mask character (like ssh-keygen).
func readPasswordWithMask(prompt string, maskChar rune) ([]byte, error) {
	// Display the prompt
	fmt.Print(prompt)

	// Set terminal into raw mode to manually handle input
	oldState, err := term.MakeRaw(int(syscall.Stdin))
	if err != nil {
		return nil, err
	}
	defer term.Restore(int(syscall.Stdin), oldState)

	// Make sure terminal state is restored if interrupted
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer signal.Stop(c)

	go func() {
		<-c
		term.Restore(int(syscall.Stdin), oldState)
		os.Exit(1)
	}()

	var password []rune
	for {
		// Read one character at a time
		var buf [1]byte
		n, err := os.Stdin.Read(buf[:])
		if err != nil || n == 0 {
			return nil, err
		}

		// Handle newline (Enter key) to finish input
		if buf[0] == '\n' || buf[0] == '\r' {
			fmt.Println()
			break
		}

		// Handle backspace (ASCII 127 or Ctrl+H)
		if buf[0] == 127 || buf[0] == 8 {
			if len(password) > 0 {
				password = password[:len(password)-1]
				// Move cursor back, print space to erase mask, move cursor back again
				fmt.Print("\b \b")
			}
		} else {
			// Append typed character and print mask character (like `*` or `🔑`)
			password = append(password, rune(buf[0]))
			fmt.Print(string(maskChar))
		}
	}

	return password, nil
}
*/
