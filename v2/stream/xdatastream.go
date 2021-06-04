package stream

import (
	"github.com/alpacahq/alpaca-trade-api-go/common"
)

type XDataStream struct {
	dataStream *datav2stream

}

func (xs *XDataStream) SetDataUrl(dataUrl string) {
	xs.dataStream.SetDataStreamUrl(dataUrl)
}

func (xs *XDataStream) Init() {
	xs.dataStream = newDatav2Stream()
}

// UseFeed sets the feed used by the data v2 stream. Supported feeds: iex, sip.
func (xs *XDataStream) UseFeed(feed string) error {
	return xs.dataStream.useFeed(feed)
}

// SubscribeTrades issues a subscribe command to the given symbols and
// registers the handler to be called for each trade.
func (xs *XDataStream) SubscribeTrades(handler func(trade Trade), symbols ...string) error {
	return xs.dataStream.subscribeTrades(handler, symbols...)
}

// SubscribeQuotes issues a subscribe command to the given symbols and
// registers the handler to be called for each quote.
func (xs *XDataStream) SubscribeQuotes(handler func(quote Quote), symbols ...string) error {
	return xs.dataStream.subscribeQuotes(handler, symbols...)
}

// SubscribeBars issues a subscribe command to the given symbols and
// registers the handler to be called for each bar.
func (xs *XDataStream) SubscribeBars(cApiKey *common.APIKey, handler func(bar Bar), symbols ...string) error {
	xs.dataStream.apiKey = cApiKey
	return xs.dataStream.subscribeBars(handler, symbols...)
}

// UnsubscribeTrades issues an unsubscribe command for the given trade symbols
func (xs *XDataStream) UnsubscribeTrades(symbols ...string) error {
	return xs.dataStream.unsubscribe(symbols, nil, nil)
}

// UnsubscribeQuotes issues an unsubscribe command for the given quote symbols
func (xs *XDataStream) UnsubscribeQuotes(symbols ...string) error {
	return xs.dataStream.unsubscribe(nil, symbols, nil)
}

// UnsubscribeBars issues an unsubscribe command for the given bar symbols
func (xs *XDataStream) UnsubscribeBars(symbols ...string) error {
	return xs.dataStream.unsubscribe(nil, nil, symbols)
}

// Close gracefully closes all streams
func (xs *XDataStream) Close() error {
	var dataErr error
	if xs.dataStream != nil {
		dataErr = xs.dataStream.close(true)
	}
	return dataErr
}
