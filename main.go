package main

import (
	"fmt"
	"time"

	"eth-tracker/db"
	"eth-tracker/net"
	"eth-tracker/utils"
	"github.com/ethereum/go-ethereum/common"
)

var (
	database *db.Database

	reporter *utils.Reporter
)

func main() {
	database = db.New()

	reporter = utils.NewReporter(10000, 60*time.Second, 0, func(rs utils.ReporterState) string {
		return fmt.Sprintf("Tracked [%d] ETH blocks in [%.2fs], speed [%.2fblks/s]", rs.CountInc, rs.ElapsedTime, float64(rs.CountInc)/rs.ElapsedTime)
	})

	for {
		doTrackEthUSDT()
	}
}

func doTrackEthUSDT() {
	nowBlockNumber, err := net.EthBlockNumber()
	if err != nil {
		time.Sleep(12 * time.Second)
		return
	}

	trackedBlockNum := database.GetLastTrackedEthBlockNum()

	n := uint64(1)
	if nowBlockNumber-trackedBlockNum > 100 {
		n = uint64(100)
	} else if nowBlockNumber == trackedBlockNum {
		time.Sleep(1 * time.Second)
		return
	}

	ethLogs, err := net.EthGetLogs(
		trackedBlockNum+1,
		trackedBlockNum+n,
		common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
		[][]common.Hash{{
			common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"), // Transfer(address,address,uint256)
			common.HexToHash("0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a"), // Issue(uint256)
			common.HexToHash("0x702d5967f45f6513a38ffc42d6ba9bf230bd40e8f53b16363c7eb4fd2deb9a44"), // Redeem(uint256)
			common.HexToHash("0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6"), // DestroyedBlackFunds(address,uint256)
		}})

	if err != nil {
		time.Sleep(12 * time.Second)
		return
	}

	for _, log := range ethLogs {
		database.ProcessEthUSDTTransferLog(log)

		if log.Topics[0].Hex() != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" {
			var logType string
			switch log.Topics[0].Hex() {
			case "0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a":
				logType = "Issue"
			case "0x702d5967f45f6513a38ffc42d6ba9bf230bd40e8f53b16363c7eb4fd2deb9a44":
				logType = "Redeem"
			case "0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6":
				logType = "DestroyedBlackFunds"
			}
			fmt.Printf("%s log found: %s\n", logType, log.TxHash.Hex())
		}
	}

	if len(ethLogs) == 0 {
		database.SetLastTrackedBlockNum(trackedBlockNum + n)
	}

	if shouldReport, reportContent := reporter.Add(int(n)); shouldReport {
		nowBlockNumber, _ := net.EthBlockNumber()
		trackedBlockNumber := database.GetLastTrackedEthBlockNum()
		fmt.Printf("%s, tracking from [%d] to [%d], left [%d], current total/dirty users [%d/%d]\n",
			reportContent, trackedBlockNumber, nowBlockNumber, nowBlockNumber-trackedBlockNumber, database.GetUsersCount(), database.GetDirtyUsersCount())
	}
}
