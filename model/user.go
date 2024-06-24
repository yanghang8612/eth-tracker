package model

type USDTUser struct {
	ID                uint   `gorm:"primaryKey"`
	Address           string `gorm:"size:42;index"`
	Amount            uint64 `gorm:"index"`
	TransferIn        uint
	TransferOut       uint
	ShouldFlushIntoDB bool `gorm:"-:all"`
}

func (e *USDTUser) Add(o *USDTUser) {
	e.Amount += o.Amount
	e.TransferIn += o.TransferIn
	e.TransferOut += o.TransferOut
}
