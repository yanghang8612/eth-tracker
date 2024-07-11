package net

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-resty/resty/v2"
)

const (
	EthJsonRpcEndpoint = "http://localhost:8545/"
	EtherScan          = "https://api.etherscan.io/"
)

var (
	client     = resty.New()
	ethClient  *ethclient.Client
	FutureTime = errors.New("future time")
)

func init() {
	var err error
	ethClient, err = ethclient.Dial(EthJsonRpcEndpoint)
	if err != nil {
	}
}

func EthBlockNumber() (uint64, error) {
	return ethClient.BlockNumber(context.Background())
}

func EthBlockNumberByTime(timestamp int64) (uint64, error) {
	resp, err := client.R().Get(EtherScan +
		"api?module=block&action=getblocknobytime&closest=after&timestamp=" +
		strconv.FormatInt(timestamp, 10) + "&apikey=82SMH9HIUESXN4IPSFA237VHIMHQB1AQSI")

	if err != nil {
		return 0, err
	} else {
		var respStruct struct {
			Status  string `json:"status"`
			Message string `json:"message"`
			Result  string `json:"result"`
		}

		err = json.Unmarshal(resp.Body(), &respStruct)
		if err != nil {
			return 0, err
		}

		if respStruct.Status == "1" {
			blockNumber, err := strconv.ParseUint(respStruct.Result, 10, 64)
			if err != nil {
				return 0, err
			}
			return blockNumber, nil
		} else {
			fmt.Printf("Etherscan error: %s - %s\n", respStruct.Message, respStruct.Result)
			return 0, FutureTime
		}
	}
}

func EthGetHeaderByNumber(blockNumber uint64) (*types.Header, error) {
	return ethClient.HeaderByNumber(context.Background(), new(big.Int).SetUint64(blockNumber))
}

func EthGetLogs(fromBlock, toBlock uint64, address common.Address, topics [][]common.Hash) ([]types.Log, error) {
	return ethClient.FilterLogs(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: []common.Address{address},
		Topics:    topics,
	})
}
