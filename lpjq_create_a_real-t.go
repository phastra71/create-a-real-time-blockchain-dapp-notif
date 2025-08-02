package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Notification struct {
	BlockNumber uint64 `json:"block_number"`
	TxHash      string `json:"tx_hash"`
	TxFrom      string `json:"tx_from"`
	TxTo        string `json:"tx_to"`
	TxValue     string `json:"tx_value"`
}

func main() {
	// Dgraph client
	dg, err := dgo.NewDgraphClient(api.NewClient("localhost:9080", nil))
	if err != nil {
		log.Fatal(err)
	}
	defer dg.Close()

	// Ethereum client
	client, err := ethclient.Dial("https://mainnet.infura.io/v3/YOUR_PROJECT_ID")
	if err != nil {
		log.Fatal(err)
	}

	// Subscribe to new heads
	headers := make(chan *types.Header)
	sub, err := client.SubscribeNewHead(context.Background(), headers)
	if err != nil {
		log.Fatal(err)
	}

	// Listen for new blocks
	for {
		select {
		case header := <-headers:
			// Get block transactions
			txHashes, err := client.TransactionIndexes(context.Background(), header.Number.Uint64())
			if err != nil {
				log.Fatal(err)
			}
			for _, txHash := range txHashes {
				tx, _, err := client.TransactionByHash(context.Background(), common.HexToHash(txHash))
				if err != nil {
					log.Fatal(err)
				}

				// Create a new notification
				notification := Notification{
					BlockNumber: header.Number.Uint64(),
					TxHash:      tx.Hash().Hex(),
					TxFrom:      tx.From().Hex(),
					TxTo:        tx.To().Hex(),
					TxValue:     tx.Value().String(),
				}

				// Marshal notification to JSON
				jsonBytes, err := json.Marshal(notification)
				if err != nil {
					log.Fatal(err)
				}

				// Store notification in Dgraph
				mu := &api.Mutation{
					SetJson: jsonBytes,
				}
			 txn := dg.NewTxn()
				err = txn.Mutate(context.Background(), mu)
				if err != nil {
					log.Fatal(err)
				}
				err = txn.Commit(context.Background())
				if err != nil {
					log.Fatal(err)
				}

				// Log notification
				fmt.Printf("New notification: %+v\n", notification)
			}
		}
	}
}