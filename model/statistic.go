package model

type ERC20Statistic struct {
	ID               uint   `gorm:"primaryKey" json:"-"`
	Date             string `gorm:"size:6;index" json:"date,omitempty"`
	Address          string `gorm:"index" json:"address"`
	HistoricalHolder int
	ActualHolder     int
	NewFrom          int
	NewTo            int
}
