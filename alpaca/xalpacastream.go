package alpaca

import (
	"github.com/alpacahq/alpaca-trade-api-go/common"
	"log"
)

var (
	defaultStreamUrl  = "https://api.alpaca.markets"
)

type XAlpacaStream struct {
	alpacaStream *Stream
}

func (xs *XAlpacaStream) Init() {
	xs.alpacaStream = NewAlpacaStream(defaultStreamUrl)
}

func (xs *XAlpacaStream) SetStreamUrl(streamUrl string) {
	xs.alpacaStream.SetBaseStream(streamUrl)
}

// SubscribeTradeUpdates issues a subscribe command to the user's trade updates and
// registers the handler to be called for each update.
func (xs *XAlpacaStream) SubscribeTradeUpdates(ky *common.APIKey, handler func(update TradeUpdate)) error {
	return xs.alpacaStream.Subscribe(ky, TradeUpdates, func(msg interface{}) {
		update, ok := msg.(TradeUpdate)
		if !ok {
			log.Printf("unexpected trade update: %v", msg)
			return
		}
		handler(update)
	})
}

// UnsubscribeTradeUpdates issues an unsubscribe command for the user's trade updates
func (xs *XAlpacaStream) UnsubscribeTradeUpdates() error {
	return xs.alpacaStream.Unsubscribe(TradeUpdates)
}

// Close gracefully closes all streams
func (xs *XAlpacaStream) Close() error {
	var alpacaErr error
	if xs.alpacaStream != nil {
		alpacaErr = xs.alpacaStream.Close()
	}
	return alpacaErr
}
