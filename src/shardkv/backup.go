package shardkv

import (
  "crypto/aes"
  "crypto/cipher"
  "crypto/rand"
  "encoding/base64"
//  "fmt"
  "io"
  "bytes"
  "encoding/gob"
  "crypto/md5"
  "encoding/hex"
)

type BackupEntry struct {
  Key []byte
  Hash string
  Entry string
}

/*
func main() {
    key := []byte("a very very very very secret key") // 32 bytes
    plaintext := []byte("some really really really long plaintext")
    fmt.Printf("%s\n", plaintext)
    ciphertext := encrypt(key, plaintext)
    fmt.Printf("%x\n", ciphertext)
    result := decrypt(key, ciphertext)
    fmt.Printf("%s\n", result)
}
*/

func GetMD5Hash(text string) string {
    hasher := md5.New()
    hasher.Write([]byte(text))
    return hex.EncodeToString(hasher.Sum(nil))
}

type OpWithSeq struct {
  Seq int
  Operation Op
}

func gobEncodeBase64(value OpWithSeq) string {
  var buffer bytes.Buffer 
  enc := gob.NewEncoder(&buffer)
  enc.Encode(value)
  return encodeBase64(buffer.Bytes())
}

func gobDecodeBase64(encoded string) OpWithSeq {
  buffer := bytes.NewBufferString(encoded)
  x := OpWithSeq{}
  dec := gob.NewDecoder(buffer)
  dec.Decode(&x)
  return x
}
// See recommended IV creation from ciphertext below
//var iv = []byte{35, 46, 57, 24, 85, 35, 24, 74, 87, 35, 88, 98, 66, 32, 14, 05}


func encodeBase64(b []byte) string {
    return base64.StdEncoding.EncodeToString(b)
}

func decodeBase64(s string) []byte {
    data, err := base64.StdEncoding.DecodeString(s)
    if err != nil {
        panic(err)
    }
    return data
}

func encrypt(key, text []byte) []byte {
    block, err := aes.NewCipher(key)
    if err != nil {
        panic(err)
    }
    b := encodeBase64(text)
    ciphertext := make([]byte, aes.BlockSize+len(b))
    iv := ciphertext[:aes.BlockSize]
    if _, err := io.ReadFull(rand.Reader, iv); err != nil {
        panic(err)
    }
    cfb := cipher.NewCFBEncrypter(block, iv)
    cfb.XORKeyStream(ciphertext[aes.BlockSize:], []byte(b))
    return ciphertext
}

func decrypt(key, text []byte) string {
    block, err := aes.NewCipher(key)
    if err != nil {
        panic(err)
    }
    if len(text) < aes.BlockSize {
        panic("ciphertext too short")
    }
    iv := text[:aes.BlockSize]
    text = text[aes.BlockSize:]
    cfb := cipher.NewCFBDecrypter(block, iv)
    cfb.XORKeyStream(text, text)
    return string(decodeBase64(string(text)))
}
