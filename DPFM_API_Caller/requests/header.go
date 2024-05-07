package requests

type Header struct {
	Attendance		int     `json:"Attendance"`
	IsCancelled		*bool	`json:"IsCancelled"`
}
