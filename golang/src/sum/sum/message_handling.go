package sum

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	messageType, err := inner.DeserializeMessageType(&msg)
	if err != nil {
		slog.Error("While deserializing message type", "err", err)
		return
	}

	switch messageType {
	case inner.TYPE_EOF_MESSAGE:
		eofCoordMessage, err := inner.DeserializeEOFMessage(&msg)
		if err != nil {
			slog.Error("While deserializing EOF message", "err", err)
			return
		}
		if eofCoordMessage.Kind != inner.EOF_START {
			slog.Error("Unexpected EOF message on input queue", "type", eofCoordMessage.Kind)
			return
		}
		if err := sum.controlExchange.Send(msg); err != nil {
			slog.Error("While sending EOF start message", "err", err)
		}

	case inner.TYPE_CLIENT_MESSAGE:
		clientID, fruitRecords, _, err := inner.DeserializeMessage(&msg)
		if err != nil {
			slog.Error("While deserializing message", "err", err)
			return
		}

		if err := sum.handleDataMessage(clientID, fruitRecords); err != nil {
			slog.Error("While handling data message", "err", err)
		}
	}
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	eofCoordMessage, err := inner.DeserializeEOFMessage(&msg)
	if err != nil {
		slog.Error("While deserializing EOF message", "err", err)
		return
	}

	switch eofCoordMessage.Kind {
	case inner.EOF_START:
		if err := sum.handleEOFStart(eofCoordMessage); err != nil {
			slog.Error("While handling EOF start", "err", err)
		}

	case inner.EOF_PROCESSED:
		sum.handleProcessedCount(eofCoordMessage)

	default:
		slog.Error("Unknown EOF message type", "type", eofCoordMessage.Kind)
	}
}
