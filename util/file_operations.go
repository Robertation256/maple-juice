package util

import (
	"os"
	"path/filepath"
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