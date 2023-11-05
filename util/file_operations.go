package util

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func EmptySdfsFolder(sdfsFolder string) error{
	dir, err := os.Open(sdfsFolder)
	if err != nil {
		return err
	}
	defer dir.Close()

	// Read all file names in the folder
	fileNames, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}

	// Remove each file
	for _, fileName := range fileNames {
		filePath := filepath.Join(sdfsFolder, fileName)
		err := os.Remove(filePath)
		if err != nil {
			return err
		} 
	}
	return nil
}

// copy file to a remote location using sftp
func CopyFileToRemote(localFilePath string, remoteFilePath string, remoteAddr string, sshConfig *ssh.ClientConfig) error {

	conn, err := ssh.Dial("tcp", remoteAddr + ":22", sshConfig)
	if err != nil {
		return(err)
	}
	
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Printf("Failed to create SFTP client: %s", err.Error())
		return(fmt.Errorf("Failed to create SFTP client: %w", err))
	}
	

	localFile, err := os.Open(localFilePath)
	if err != nil {
		return(fmt.Errorf("Failed to open local file: %w", err))
	}
	defer localFile.Close()

	remoteFile, err := client.Create(remoteFilePath)
	if err != nil {
		return(fmt.Errorf("Failed to create remote file: %w", err))
	}
	defer remoteFile.Close()

	_, err = io.Copy(remoteFile, localFile)
	if err != nil {
		return(fmt.Errorf("Failed to upload file: %w", err))
	}


	client.Close()
	return nil
}

func DeleteFile(filename string, sdfsFolder string) error{
	filePath := sdfsFolder + filename
	err := os.Remove(filePath)
    if err != nil {
        return err
    }
    return nil
}