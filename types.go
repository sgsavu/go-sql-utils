package sqlutils

type SQLConnectionInfo struct {
	Addr   string `json:"address"`
	DBName string `json:"dbName"`
	User   string `json:"user"`
	Passwd string `json:"password"`
}
