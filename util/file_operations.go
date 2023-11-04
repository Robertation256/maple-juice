package util

import (
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"os"
	"io" 
	"fmt"
)

func CopyFileToRemote(localFilePath string, remoteFilePath string, remoteAddr string, sshConfig *ssh.ClientConfig) error {
	conn, err := ssh.Dial("tcp", remoteAddr + ":22", sshConfig)
	if err != nil {
		return(err)
	}
	
	client, err := sftp.NewClient(conn)
	if err != nil {
		return(fmt.Errorf("Failed to create SFTP client: %w", err))
	}
	defer client.Close()

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

	return nil
}

func DeleteFile(localFilePath string) error{
	err := os.Remove(localFilePath)
    if err != nil {
        return err
    }
    return nil
}