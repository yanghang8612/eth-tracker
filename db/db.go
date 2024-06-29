package db

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"

	"eth-tracker/model"
	"eth-tracker/net"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	TrackedBlockNumKey = "tracked_block_num"
)

type Meta struct {
	gorm.Model
	Key string `gorm:"unique"`
	Val string
}

type TokenHolder struct {
	ID          uint   `gorm:"primaryKey"`
	Address     string `gorm:"size:42;index"`
	Amount      uint64 `gorm:"index"`
	TransferIn  uint
	TransferOut uint
	Dirty       bool `gorm:"-:all"`
}

type TokenHistoricalHolder struct {
	ID          uint   `gorm:"primaryKey"`
	Address     string `gorm:"size:42;index"`
	TransferIn  uint
	TransferOut uint
}

type Database struct {
	db *gorm.DB

	lastTrackedBlockNum  uint64
	nextDayStartBlockNum uint64

	users                  map[common.Address]*model.USDTUser
	usersFilter            *bloom.BloomFilter
	userCountWithNoBalance int

	historicalHolder int
	dayStatOfUSDT    model.ERC20Statistic

	TotalTestCount    uint64
	TotalMatchedCount uint64
	TotalReloadCount  uint64
}

func New() *Database {
	dsn := fmt.Sprintf("root:Root1234!@tcp(127.0.0.1:3306)/usdt?charset=utf8mb4&parseTime=True&loc=Local")
	db, dbErr := gorm.Open(mysql.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger:                 logger.Discard,
	})
	if dbErr != nil {
		panic(dbErr)
	}

	dbErr = db.AutoMigrate(&model.USDTUser{})
	if dbErr != nil {
		panic(dbErr)
	}

	dbErr = db.AutoMigrate(&model.ERC20Statistic{})
	if dbErr != nil {
		panic(dbErr)
	}

	dbErr = db.AutoMigrate(&model.Meta{})
	if dbErr != nil {
		panic(dbErr)
	}

	var trackedBlockNumMeta model.Meta
	db.Where(model.Meta{Key: model.TrackedBlockNumKey}).Attrs(model.Meta{Val: strconv.Itoa(4634748)}).FirstOrCreate(&trackedBlockNumMeta)
	trackedBlockNum, _ := strconv.Atoi(trackedBlockNumMeta.Val)

	var nextDayStartBlockNumMeta model.Meta
	db.Where(model.Meta{Key: model.NextDayStartBlockNum}).Attrs(model.Meta{Val: strconv.Itoa(4640667)}).FirstOrCreate(&nextDayStartBlockNumMeta)
	nextDayStartBlockNum, _ := strconv.Atoi(nextDayStartBlockNumMeta.Val)

	database := &Database{
		db: db,

		lastTrackedBlockNum:  uint64(trackedBlockNum),
		nextDayStartBlockNum: uint64(nextDayStartBlockNum),

		users:       make(map[common.Address]*model.USDTUser),
		usersFilter: bloom.NewWithEstimates(50_000_000, 0.02),
	}

	database.loadUsers()
	if database.lastTrackedBlockNum == 4634748 {
		database.users[common.HexToAddress("0x36928500bc1dcd7af6a2b4008875cc336b927d57")] = &model.USDTUser{
			Amount: 100000000000,
		}
		database.dayStatOfUSDT = model.ERC20Statistic{
			Date:    "171128",
			Address: "0xdac17f958d2ee523a2206206994597c13d831ec7",
		}
	} else {
		database.dayStatOfUSDT = database.GetERC20DayStat("-")
		database.historicalHolder = database.dayStatOfUSDT.HistoricalHolder
	}

	return database
}

func (db *Database) loadUsers() {
	fmt.Printf("Start loading users from db\n")

	report := make(map[int]bool)
	users := make([]*model.USDTUser, 0)
	result := db.db.FindInBatches(&users, 100, func(_ *gorm.DB, _ int) error {
		for _, user := range users {
			db.usersFilter.Add(hexutil.MustDecode(user.Address))

			if user.Amount > 0 {
				db.users[common.HexToAddress(user.Address)] = user
				user.Address = ""
			}
		}

		phase := len(db.users) / 200_000
		if _, ok := report[phase]; !ok {
			report[phase] = true
			fmt.Printf("Loaded [%d] users from db\n", len(db.users))
		}

		return nil
	})

	fmt.Printf("Loaded [%d] of [%d] users from db\n", len(db.users), result.RowsAffected)
}

func (db *Database) Close() {
	db.flushUsersToDB(true)

	underDB, _ := db.db.DB()
	_ = underDB.Close()
}

func (db *Database) GetLastTrackedEthBlockNum() uint64 {
	return db.lastTrackedBlockNum
}

func (db *Database) GetUsersCount() int {
	return len(db.users)
}

func (db *Database) GetUserCountWithNoBalance() int {
	return db.userCountWithNoBalance
}

func (db *Database) GetERC20DayStat(date string) model.ERC20Statistic {
	var dayStat model.ERC20Statistic
	db.db.Where("date = ?", date).First(dayStat)
	return dayStat
}

func (db *Database) SetLastTrackedBlockNum(blockNum uint64) {
	db.lastTrackedBlockNum = blockNum

	if blockNum >= db.nextDayStartBlockNum { // Catching up
		currentDay := db.updateDayStat()

		// If we catch up with the current day, we should mark next day's start block number to max
		if currentDay.Format("060102") == time.Now().Format("060102") {
			db.nextDayStartBlockNum = 0xffffffffffffffff
			fmt.Printf("Catch up to today [%s]\n", currentDay)
			return
		}

		nextDayStartBlockNum, _ := net.EthBlockNumberByTime(currentDay.AddDate(0, 0, 1).Unix())
		db.nextDayStartBlockNum = nextDayStartBlockNum
		fmt.Printf("Current day [%s] end block num [%d]\n", currentDay.Format("060102"), nextDayStartBlockNum)
	} else { // Caught up
		header, _ := net.EthGetHeaderByNumber(blockNum)
		date := generateDate(int64(header.Time))

		if date != db.dayStatOfUSDT.Date {
			db.updateDayStat()
		}
	}
}

func (db *Database) ProcessEthUSDTTransferLog(log types.Log) {
	var (
		from      common.Address
		to        common.Address
		emptyFrom bool
		emptyTo   bool
		amount    uint64
	)

	switch log.Topics[0].Hex() {
	case "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
		from = common.BytesToAddress(log.Topics[1].Bytes())
		to = common.BytesToAddress(log.Topics[2].Bytes())
		amount = convertBytesToUint64(log.Data)
	case "0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a":
		emptyFrom = true
		to = common.HexToAddress("0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828")
		amount = convertBytesToUint64(log.Data)
	case "0x702d5967f45f6513a38ffc42d6ba9bf230bd40e8f53b16363c7eb4fd2deb9a44":
		from = common.HexToAddress("0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828")
		emptyTo = true
		amount = convertBytesToUint64(log.Data)
	case "0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6":
		from = common.BytesToAddress(log.Data[12:32])
		emptyTo = true
		amount = convertBytesToUint64(log.Data[32:])
	}

	if amount == 0 {
		goto finish
	}

	if !emptyFrom {
		if _, ok := db.users[from]; !ok {
			// Actually this can not happen
			db.users[from] = &model.USDTUser{}
		}

		// New from user should exist in memory, because he must have balance
		if !emptyTo && db.users[from].TransferOut == 0 {
			db.dayStatOfUSDT.NewFrom += 1
		}

		db.users[from].Amount -= amount
		db.users[from].TransferOut += 1
		db.users[from].LastUpdateAt = log.BlockNumber

		if db.users[from].Amount == 0 && !db.users[from].ShouldFlushIntoDB {
			db.users[from].ShouldFlushIntoDB = true
			db.userCountWithNoBalance += 1
		}

		if db.users[from].Amount < 0 {
			fmt.Printf("User [%s] has negative amount [%d], tx [%s]\n", from, db.users[from].Amount, log.TxHash)
		}
	}

	if !emptyTo {
		if !db.hasUSDTUser(to) {
			db.users[to] = &model.USDTUser{}
			db.usersFilter.Add(to.Bytes())
			db.dayStatOfUSDT.NewTo += 1
		}

		db.users[to].Amount += amount
		db.users[to].TransferIn += 1
		db.users[to].LastUpdateAt = log.BlockNumber

		if db.users[to].ShouldFlushIntoDB {
			db.users[to].ShouldFlushIntoDB = false
			db.userCountWithNoBalance -= 1
		}
	}

	if db.userCountWithNoBalance >= 2_000_000 {
		db.userCountWithNoBalance = db.flushUsersToDB(false)
	}

finish:
	if log.BlockNumber != db.lastTrackedBlockNum {
		db.SetLastTrackedBlockNum(log.BlockNumber)
	}
}

func (db *Database) hasUSDTUser(addr common.Address) bool {
	if _, ok := db.users[addr]; ok {
		return true
	}

	db.TotalTestCount += 1
	if db.usersFilter.Test(addr.Bytes()) {
		db.TotalMatchedCount += 1

		user := &model.USDTUser{Address: addr.Hex()}
		result := db.db.First(user)

		if result.Error == nil {
			db.users[addr] = user
			db.users[addr].Address = ""
			db.TotalReloadCount += 1
			return true
		}
	}
	return false
}

func (db *Database) flushUsersToDB(force bool) int {
	usersToSave := make([]*model.USDTUser, 0)

	if force {
		db.db.Model(&model.Meta{}).Where(model.Meta{Key: model.TrackedBlockNumKey}).Update("val", strconv.Itoa(int(db.lastTrackedBlockNum)))
		db.db.Model(&model.Meta{}).Where(model.Meta{Key: model.NextDayStartBlockNum}).Update("val", strconv.Itoa(int(db.nextDayStartBlockNum)))
		db.dayStatOfUSDT.Date = "-"
		db.dayStatOfUSDT.HistoricalHolder = db.historicalHolder
		db.db.Save(db.dayStatOfUSDT)

		for addr, user := range db.users {
			user.Address = addr.Hex()
			usersToSave = append(usersToSave, user)
		}
		db.saveUsers(usersToSave)

		return 0
	}

	usersWithNoBalance := make([]*model.USDTUser, 0)
	for addr, user := range db.users {
		if user.Amount == 0 {
			user.Address = addr.Hex()
			usersWithNoBalance = append(usersWithNoBalance, user)
		}
	}

	sort.Slice(usersWithNoBalance, func(i, j int) bool {
		return usersWithNoBalance[i].LastUpdateAt < usersWithNoBalance[j].LastUpdateAt
	})

	for i := 0; i < 1_000_000; i++ {
		usersToSave = append(usersToSave, usersWithNoBalance[i])
		delete(db.users, common.HexToAddress(usersWithNoBalance[i].Address))
	}

	db.saveUsers(usersToSave)

	for i := 1_000_000; i < len(usersWithNoBalance); i++ {
		usersWithNoBalance[i].Address = ""
	}

	return len(usersWithNoBalance) - 1_000_000
}

func (db *Database) saveUsers(users []*model.USDTUser) {
	fmt.Printf("Start saving users to DB, total [%d]\n", len(users))

	savedCount := 0
	usersToSave := make([]*model.USDTUser, 0)

	for _, user := range users {
		usersToSave = append(usersToSave, user)
		if len(usersToSave) == 100 {
			db.db.Save(usersToSave)
			usersToSave = make([]*model.USDTUser, 0)
		}
		savedCount += 1

		if savedCount%200_000 == 0 {
			fmt.Printf("Saved %d users to DB\n", savedCount)
		}
	}
	db.db.Save(usersToSave)
	savedCount += len(usersToSave)
	fmt.Printf("Finish saving [%d] users", savedCount)
}

func (db *Database) updateDayStat() time.Time {
	db.historicalHolder += db.dayStatOfUSDT.NewTo
	db.dayStatOfUSDT.HistoricalHolder = db.historicalHolder
	for _, user := range db.users {
		if user.Amount > 0 {
			db.dayStatOfUSDT.ActualHolder += 1
		}
	}
	db.db.Save(&db.dayStatOfUSDT)

	lastDay, _ := time.Parse("060102", db.dayStatOfUSDT.Date)
	currentDay := lastDay.AddDate(0, 0, 1)

	db.dayStatOfUSDT = model.ERC20Statistic{
		Date:    currentDay.Format("060102"),
		Address: "0xdac17f958d2ee523a2206206994597c13d831ec7",
	}

	return currentDay
}

func generateDate(ts int64) string {
	return time.Unix(ts, 0).In(time.FixedZone("UTC", 0)).Format("060102")
}

func convertBytesToUint64(bytes []byte) uint64 {
	bi := new(big.Int).SetBytes(bytes)
	return bi.Uint64()
}
