package util

import (
	"os"
	"path/filepath"
	"cs425-mp4/config"
	"log"
	"errors"
	"os/exec"
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


func DeleteFile(filename string, sdfsFolder string) error{
	filePath := sdfsFolder + filename
	err := os.Remove(filePath)
    if err != nil {
        return err
    }
    return nil
}

func FileInSdfsFolder(filename string) bool{
	filePath := config.SdfsFileDir + filename
	_, err := os.Stat(filePath)
    if err == nil {
        return true
    }
    if errors.Is(err, os.ErrNotExist) {
        return false
    }
	// some other errors occured, like permission denined
	log.Println("Error checking if file is in sdfs folder: ", err)
    return false
}

func CopyFileFromSdfsToLocal(sdfsName, localName string) error {
	sdfsPath := config.SdfsFileDir + sdfsName
	localPath := config.LocalFileDir + localName
	cmd := exec.Command("cp", sdfsPath, localPath)

	_, err := cmd.CombinedOutput()

	if err != nil {
		log.Println("Error while copying from /sdfs to /local:", err)
	}

	return err
}