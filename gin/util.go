package gin

import "os"

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
func RemoveFiles(files []string)error{
	for _, path := range files{
		err := RemoveFile(path)
		if err!=nil{
			return err
		}
	}
	return nil
}
func RemoveFile(path string)error{
	exists, err := PathExists(path)
	if exists{
		err = os.Remove(path)
	}
	return err
}