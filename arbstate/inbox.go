// Copyright 2021-2022, Offchain Labs, Inc.
// For license information, see https://github.com/nitro/blob/master/LICENSE

package arbstate

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"

	"github.com/offchainlabs/nitro/arbcompress"
	"github.com/offchainlabs/nitro/arbos/arbosState"
	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbos/l1pricing"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/das/dastree"
	"github.com/offchainlabs/nitro/util/blobs"
	"github.com/offchainlabs/nitro/zeroheavy"
)

type InboxBackend interface {
	PeekSequencerInbox() ([]byte, common.Hash, error)

	GetSequencerInboxPosition() uint64
	AdvanceSequencerInbox()

	GetPositionWithinMessage() uint64
	SetPositionWithinMessage(pos uint64)

	ReadDelayedInbox(seqNum uint64) (*arbostypes.L1IncomingMessage, error)
}

type BlobReader interface {
	GetBlobs(
		ctx context.Context,
		batchBlockHash common.Hash,
		versionedHashes []common.Hash,
	) ([]kzg4844.Blob, error)
	Initialize(ctx context.Context) error
}

type sequencerMessage struct {
	minTimestamp         uint64
	maxTimestamp         uint64
	minL1Block           uint64
	maxL1Block           uint64
	afterDelayedMessages uint64
	segments             [][]byte
}

const MaxDecompressedLen int = 1024 * 1024 * 16 // 16 MiB
const maxZeroheavyDecompressedLen = 101*MaxDecompressedLen/100 + 64
const MaxSegmentsPerSequencerMessage = 100 * 1024
const MinLifetimeSecondsForDataAvailabilityCert = 7 * 24 * 60 * 60 // one week

func parseSequencerMessage(ctx context.Context, batchNum uint64, batchBlockHash common.Hash, data []byte, daProviders []DataAvailabilityProvider, keysetValidationMode KeysetValidationMode) (*sequencerMessage, error) {
	if len(data) < 40 {
		return nil, errors.New("sequencer message missing L1 header")
	}
	parsedMsg := &sequencerMessage{
		minTimestamp:         binary.BigEndian.Uint64(data[:8]),
		maxTimestamp:         binary.BigEndian.Uint64(data[8:16]),
		minL1Block:           binary.BigEndian.Uint64(data[16:24]),
		maxL1Block:           binary.BigEndian.Uint64(data[24:32]),
		afterDelayedMessages: binary.BigEndian.Uint64(data[32:40]),
		segments:             [][]byte{},
	}
	payload := data[40:]

	// Stage 0: Check if our node is out of date and we don't understand this batch type
	// If the parent chain sequencer inbox smart contract authenticated this batch,
	// an unknown header byte must mean that this node is out of date,
	// because the smart contract understands the header byte and this node doesn't.
	if len(payload) > 0 && IsL1AuthenticatedMessageHeaderByte(payload[0]) && !IsKnownHeaderByte(payload[0]) {
		return nil, fmt.Errorf("%w: batch has unsupported authenticated header byte 0x%02x", arbosState.ErrFatalNodeOutOfDate, payload[0])
	}

	// Stage 1: Extract the payload from any data availability header.
	// It's important that multiple DAS strategies can't both be invoked in the same batch,
	// as these headers are validated by the sequencer inbox and not other DASs.
	// We try to extract payload from the first occuring valid DA provider in the daProviders list
	if len(payload) > 0 {
		foundDA := false
		var err error
		for _, provider := range daProviders {
			if provider != nil && provider.IsValidHeaderByte(payload[0]) {
				payload, err = provider.RecoverPayloadFromBatch(ctx, batchNum, batchBlockHash, data, nil, keysetValidationMode)
				if err != nil {
					return nil, err
				}
				if payload == nil {
					return parsedMsg, nil
				}
				foundDA = true
				break
			}
		}

		if !foundDA {
			if IsDASMessageHeaderByte(payload[0]) {
				log.Error("No DAS Reader configured, but sequencer message found with DAS header")
			} else if IsBlobHashesHeaderByte(payload[0]) {
				return nil, errors.New("blob batch payload was encountered but no BlobReader was configured")
			}
		}
	}

	// At this point, `payload` has not been validated by the sequencer inbox at all.
	// It's not safe to trust any part of the payload from this point onwards.

	// Stage 2: If enabled, decode the zero heavy payload (saves gas based on calldata charging).
	if len(payload) > 0 && IsZeroheavyEncodedHeaderByte(payload[0]) {
		pl, err := io.ReadAll(io.LimitReader(zeroheavy.NewZeroheavyDecoder(bytes.NewReader(payload[1:])), int64(maxZeroheavyDecompressedLen)))
		if err != nil {
			log.Warn("error reading from zeroheavy decoder", err.Error())
			return parsedMsg, nil
		}
		payload = pl
	}

	// Stage 3: Decompress the brotli payload and fill the parsedMsg.segments list.
	if len(payload) > 0 && IsBrotliMessageHeaderByte(payload[0]) {
		decompressed, err := arbcompress.Decompress(payload[1:], MaxDecompressedLen)
		if err == nil {
			reader := bytes.NewReader(decompressed)
			stream := rlp.NewStream(reader, uint64(MaxDecompressedLen))
			for {
				var segment []byte
				err := stream.Decode(&segment)
				if err != nil {
					if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
						log.Warn("error parsing sequencer message segment", "err", err.Error())
					}
					break
				}
				if len(parsedMsg.segments) >= MaxSegmentsPerSequencerMessage {
					log.Warn("too many segments in sequence batch")
					break
				}
				parsedMsg.segments = append(parsedMsg.segments, segment)
			}
		} else {
			log.Warn("sequencer msg decompression failed", "err", err)
		}
	} else {
		length := len(payload)
		if length == 0 {
			log.Warn("empty sequencer message")
		} else {
			log.Warn("unknown sequencer message format", "length", length, "firstByte", payload[0])
		}

	}

	return parsedMsg, nil
}

func RecoverPayloadFromDasBatch(
	ctx context.Context,
	batchNum uint64,
	sequencerMsg []byte,
	dasReader DataAvailabilityReader,
	preimages map[arbutil.PreimageType]map[common.Hash][]byte,
	keysetValidationMode KeysetValidationMode,
) ([]byte, error) {
	var keccakPreimages map[common.Hash][]byte
	if preimages != nil {
		if preimages[arbutil.Keccak256PreimageType] == nil {
			preimages[arbutil.Keccak256PreimageType] = make(map[common.Hash][]byte)
		}
		keccakPreimages = preimages[arbutil.Keccak256PreimageType]
	}
	cert, err := DeserializeDASCertFrom(bytes.NewReader(sequencerMsg[40:]))
	if err != nil {
		log.Error("Failed to deserialize DAS message", "err", err)
		return nil, nil
	}
	version := cert.Version
	recordPreimage := func(key common.Hash, value []byte) {
		keccakPreimages[key] = value
	}

	if version >= 2 {
		log.Error("Your node software is probably out of date", "certificateVersion", version)
		return nil, nil
	}

	getByHash := func(ctx context.Context, hash common.Hash) ([]byte, error) {
		newHash := hash
		if version == 0 {
			newHash = dastree.FlatHashToTreeHash(hash)
		}

		preimage, err := dasReader.GetByHash(ctx, newHash)
		if err != nil && hash != newHash {
			log.Debug("error fetching new style hash, trying old", "new", newHash, "old", hash, "err", err)
			preimage, err = dasReader.GetByHash(ctx, hash)
		}
		if err != nil {
			return nil, err
		}

		switch {
		case version == 0 && crypto.Keccak256Hash(preimage) != hash:
			fallthrough
		case version == 1 && dastree.Hash(preimage) != hash:
			log.Error(
				"preimage mismatch for hash",
				"hash", hash, "err", ErrHashMismatch, "version", version,
			)
			return nil, ErrHashMismatch
		}
		return preimage, nil
	}

	keysetPreimage, err := getByHash(ctx, cert.KeysetHash)
	if err != nil {
		log.Error("Couldn't get keyset", "err", err)
		return nil, err
	}
	if keccakPreimages != nil {
		dastree.RecordHash(recordPreimage, keysetPreimage)
	}

	keyset, err := DeserializeKeyset(bytes.NewReader(keysetPreimage), keysetValidationMode == KeysetDontValidate)
	if err != nil {
		logLevel := log.Error
		if keysetValidationMode == KeysetPanicIfInvalid {
			logLevel = log.Crit
		}
		logLevel("Couldn't deserialize keyset", "err", err, "keysetHash", cert.KeysetHash, "batchNum", batchNum)
		return nil, nil
	}
	err = keyset.VerifySignature(cert.SignersMask, cert.SerializeSignableFields(), cert.Sig)
	if err != nil {
		log.Error("Bad signature on DAS batch", "err", err)
		return nil, nil
	}

	maxTimestamp := binary.BigEndian.Uint64(sequencerMsg[8:16])
	if cert.Timeout < maxTimestamp+MinLifetimeSecondsForDataAvailabilityCert {
		log.Error("Data availability cert expires too soon", "err", "")
		return nil, nil
	}

	dataHash := cert.DataHash
	payload, err := getByHash(ctx, dataHash)
	if err != nil {
		log.Error("Couldn't fetch DAS batch contents", "err", err)
		return nil, err
	}

	if keccakPreimages != nil {
		if version == 0 {
			treeLeaf := dastree.FlatHashToTreeLeaf(dataHash)
			keccakPreimages[dataHash] = payload
			keccakPreimages[crypto.Keccak256Hash(treeLeaf)] = treeLeaf
		} else {
			dastree.RecordHash(recordPreimage, payload)
		}
	}

	return payload, nil
}

type DataAvailabilityProvider interface {
	// IsValidHeaderByte returns true if the given headerByte has bits corresponding to the DA provider
	IsValidHeaderByte(headerByte byte) bool

	// RecoverPayloadFromBatch fetches the underlying payload from the DA provider given the batch header information
	RecoverPayloadFromBatch(
		ctx context.Context,
		batchNum uint64,
		batchBlockHash common.Hash,
		sequencerMsg []byte,
		preimages map[arbutil.PreimageType]map[common.Hash][]byte,
		keysetValidationMode KeysetValidationMode,
	) ([]byte, error)
}

// NewDAProviderDAS is generally meant to be only used by nitro.
// DA Providers should implement methods in the DataAvailabilityProvider interface independently
func NewDAProviderDAS(das DataAvailabilityReader) *dAProviderForDAS {
	return &dAProviderForDAS{
		das: das,
	}
}

type dAProviderForDAS struct {
	das DataAvailabilityReader
}

func (d *dAProviderForDAS) IsValidHeaderByte(headerByte byte) bool {
	return IsDASMessageHeaderByte(headerByte)
}

func (d *dAProviderForDAS) RecoverPayloadFromBatch(
	ctx context.Context,
	batchNum uint64,
	batchBlockHash common.Hash,
	sequencerMsg []byte,
	preimages map[arbutil.PreimageType]map[common.Hash][]byte,
	keysetValidationMode KeysetValidationMode,
) ([]byte, error) {
	return RecoverPayloadFromDasBatch(ctx, batchNum, sequencerMsg, d.das, preimages, keysetValidationMode)
}

// NewDAProviderBlobReader is generally meant to be only used by nitro.
// DA Providers should implement methods in the DataAvailabilityProvider interface independently
func NewDAProviderBlobReader(blobReader BlobReader) *dAProviderForBlobReader {
	return &dAProviderForBlobReader{
		blobReader: blobReader,
	}
}

type dAProviderForBlobReader struct {
	blobReader BlobReader
}

func (b *dAProviderForBlobReader) IsValidHeaderByte(headerByte byte) bool {
	return IsBlobHashesHeaderByte(headerByte)
}

func (b *dAProviderForBlobReader) RecoverPayloadFromBatch(
	ctx context.Context,
	batchNum uint64,
	batchBlockHash common.Hash,
	sequencerMsg []byte,
	preimages map[arbutil.PreimageType]map[common.Hash][]byte,
	keysetValidationMode KeysetValidationMode,
) ([]byte, error) {
	blobHashes := sequencerMsg[41:]
	if len(blobHashes)%len(common.Hash{}) != 0 {
		return nil, fmt.Errorf("blob batch data is not a list of hashes as expected")
	}
	versionedHashes := make([]common.Hash, len(blobHashes)/len(common.Hash{}))
	for i := 0; i*32 < len(blobHashes); i += 1 {
		copy(versionedHashes[i][:], blobHashes[i*32:(i+1)*32])
	}
	kzgBlobs, err := b.blobReader.GetBlobs(ctx, batchBlockHash, versionedHashes)
	if err != nil {
		return nil, fmt.Errorf("failed to get blobs: %w", err)
	}
	payload, err := blobs.DecodeBlobs(kzgBlobs)
	if err != nil {
		log.Warn("Failed to decode blobs", "batchBlockHash", batchBlockHash, "versionedHashes", versionedHashes, "err", err)
		return nil, nil
	}
	return payload, nil
}

type KeysetValidationMode uint8

const KeysetValidate KeysetValidationMode = 0
const KeysetPanicIfInvalid KeysetValidationMode = 1
const KeysetDontValidate KeysetValidationMode = 2

type inboxMultiplexer struct {
	backend                   InboxBackend
	delayedMessagesRead       uint64
	daProviders               []DataAvailabilityProvider
	cachedSequencerMessage    *sequencerMessage
	cachedSequencerMessageNum uint64
	cachedSegmentNum          uint64
	cachedSegmentTimestamp    uint64
	cachedSegmentBlockNumber  uint64
	cachedSubMessageNumber    uint64
	keysetValidationMode      KeysetValidationMode
}

func NewInboxMultiplexer(backend InboxBackend, delayedMessagesRead uint64, daProviders []DataAvailabilityProvider, keysetValidationMode KeysetValidationMode) arbostypes.InboxMultiplexer {
	return &inboxMultiplexer{
		backend:              backend,
		delayedMessagesRead:  delayedMessagesRead,
		daProviders:          daProviders,
		keysetValidationMode: keysetValidationMode,
	}
}

const BatchSegmentKindL2Message uint8 = 0
const BatchSegmentKindL2MessageBrotli uint8 = 1
const BatchSegmentKindDelayedMessages uint8 = 2
const BatchSegmentKindAdvanceTimestamp uint8 = 3
const BatchSegmentKindAdvanceL1BlockNumber uint8 = 4

// Pop returns the message from the top of the sequencer inbox and removes it from the queue.
// Note: this does *not* return parse errors, those are transformed into invalid messages
func (r *inboxMultiplexer) Pop(ctx context.Context) (*arbostypes.MessageWithMetadata, error) {
	if r.cachedSequencerMessage == nil {
		// Note: batchBlockHash will be zero in the replay binary, but that's fine
		bytes, batchBlockHash, realErr := r.backend.PeekSequencerInbox()
		if realErr != nil {
			return nil, realErr
		}
		r.cachedSequencerMessageNum = r.backend.GetSequencerInboxPosition()
		var err error
		r.cachedSequencerMessage, err = parseSequencerMessage(ctx, r.cachedSequencerMessageNum, batchBlockHash, bytes, r.daProviders, r.keysetValidationMode)
		if err != nil {
			return nil, err
		}
	}
	msg, err := r.getNextMsg()
	// advance even if there was an error
	if r.IsCachedSegementLast() {
		r.advanceSequencerMsg()
	} else {
		r.advanceSubMsg()
	}
	// parsing error in getNextMsg
	if msg == nil && err == nil {
		msg = &arbostypes.MessageWithMetadata{
			Message:             arbostypes.InvalidL1Message,
			DelayedMessagesRead: r.delayedMessagesRead,
		}
	}
	return msg, err
}

func (r *inboxMultiplexer) advanceSequencerMsg() {
	if r.cachedSequencerMessage != nil {
		r.delayedMessagesRead = r.cachedSequencerMessage.afterDelayedMessages
	}
	r.backend.SetPositionWithinMessage(0)
	r.backend.AdvanceSequencerInbox()
	r.cachedSequencerMessage = nil
	r.cachedSegmentNum = 0
	r.cachedSegmentTimestamp = 0
	r.cachedSegmentBlockNumber = 0
	r.cachedSubMessageNumber = 0
}

func (r *inboxMultiplexer) advanceSubMsg() {
	prevPos := r.backend.GetPositionWithinMessage()
	r.backend.SetPositionWithinMessage(prevPos + 1)
}

func (r *inboxMultiplexer) IsCachedSegementLast() bool {
	seqMsg := r.cachedSequencerMessage
	// we issue delayed messages until reaching afterDelayedMessages
	if r.delayedMessagesRead < seqMsg.afterDelayedMessages {
		return false
	}
	for segmentNum := int(r.cachedSegmentNum) + 1; segmentNum < len(seqMsg.segments); segmentNum++ {
		segment := seqMsg.segments[segmentNum]
		if len(segment) == 0 {
			continue
		}
		kind := segment[0]
		if kind == BatchSegmentKindL2Message || kind == BatchSegmentKindL2MessageBrotli {
			return false
		}
		if kind == BatchSegmentKindDelayedMessages {
			return false
		}
	}
	return true
}

// Returns a message, the segment number that had this message, and real/backend errors
// parsing errors will be reported to log, return nil msg and nil error
func (r *inboxMultiplexer) getNextMsg() (*arbostypes.MessageWithMetadata, error) {
	targetSubMessage := r.backend.GetPositionWithinMessage()
	seqMsg := r.cachedSequencerMessage
	segmentNum := r.cachedSegmentNum
	timestamp := r.cachedSegmentTimestamp
	blockNumber := r.cachedSegmentBlockNumber
	submessageNumber := r.cachedSubMessageNumber
	var segment []byte
	for {
		if segmentNum >= uint64(len(seqMsg.segments)) {
			break
		}
		segment = seqMsg.segments[int(segmentNum)]
		if len(segment) == 0 {
			segmentNum++
			continue
		}
		segmentKind := segment[0]
		if segmentKind == BatchSegmentKindAdvanceTimestamp || segmentKind == BatchSegmentKindAdvanceL1BlockNumber {
			rd := bytes.NewReader(segment[1:])
			advancing, err := rlp.NewStream(rd, 16).Uint64()
			if err != nil {
				log.Warn("error parsing sequencer advancing segment", "err", err)
				segmentNum++
				continue
			}
			if segmentKind == BatchSegmentKindAdvanceTimestamp {
				timestamp += advancing
			} else if segmentKind == BatchSegmentKindAdvanceL1BlockNumber {
				blockNumber += advancing
			}
			segmentNum++
		} else if submessageNumber < targetSubMessage {
			segmentNum++
			submessageNumber++
		} else {
			break
		}
	}
	r.cachedSegmentNum = segmentNum
	r.cachedSegmentTimestamp = timestamp
	r.cachedSegmentBlockNumber = blockNumber
	r.cachedSubMessageNumber = submessageNumber
	if timestamp < seqMsg.minTimestamp {
		timestamp = seqMsg.minTimestamp
	} else if timestamp > seqMsg.maxTimestamp {
		timestamp = seqMsg.maxTimestamp
	}
	if blockNumber < seqMsg.minL1Block {
		blockNumber = seqMsg.minL1Block
	} else if blockNumber > seqMsg.maxL1Block {
		blockNumber = seqMsg.maxL1Block
	}
	if segmentNum >= uint64(len(seqMsg.segments)) {
		// after end of batch there might be "virtual" delayedMsgSegments
		log.Warn("reading virtual delayed message segment", "delayedMessagesRead", r.delayedMessagesRead, "afterDelayedMessages", seqMsg.afterDelayedMessages)
		segment = []byte{BatchSegmentKindDelayedMessages}
	} else {
		segment = seqMsg.segments[int(segmentNum)]
	}
	if len(segment) == 0 {
		log.Error("empty sequencer message segment", "sequence", r.cachedSegmentNum, "segmentNum", segmentNum)
		return nil, nil
	}
	kind := segment[0]
	segment = segment[1:]
	var msg *arbostypes.MessageWithMetadata
	if kind == BatchSegmentKindL2Message || kind == BatchSegmentKindL2MessageBrotli {

		if kind == BatchSegmentKindL2MessageBrotli {
			decompressed, err := arbcompress.Decompress(segment, arbostypes.MaxL2MessageSize)
			if err != nil {
				log.Info("dropping compressed message", "err", err, "delayedMsg", r.delayedMessagesRead)
				return nil, nil
			}
			segment = decompressed
		}

		msg = &arbostypes.MessageWithMetadata{
			Message: &arbostypes.L1IncomingMessage{
				Header: &arbostypes.L1IncomingMessageHeader{
					Kind:        arbostypes.L1MessageType_L2Message,
					Poster:      l1pricing.BatchPosterAddress,
					BlockNumber: blockNumber,
					Timestamp:   timestamp,
					RequestId:   nil,
					L1BaseFee:   big.NewInt(0),
				},
				L2msg: segment,
			},
			DelayedMessagesRead: r.delayedMessagesRead,
		}
	} else if kind == BatchSegmentKindDelayedMessages {
		if r.delayedMessagesRead >= seqMsg.afterDelayedMessages {
			if segmentNum < uint64(len(seqMsg.segments)) {
				log.Warn(
					"attempt to read past batch delayed message count",
					"delayedMessagesRead", r.delayedMessagesRead,
					"batchAfterDelayedMessages", seqMsg.afterDelayedMessages,
				)
			}
			msg = &arbostypes.MessageWithMetadata{
				Message:             arbostypes.InvalidL1Message,
				DelayedMessagesRead: seqMsg.afterDelayedMessages,
			}
		} else {
			delayed, realErr := r.backend.ReadDelayedInbox(r.delayedMessagesRead)
			if realErr != nil {
				return nil, realErr
			}
			r.delayedMessagesRead += 1
			msg = &arbostypes.MessageWithMetadata{
				Message:             delayed,
				DelayedMessagesRead: r.delayedMessagesRead,
			}
		}
	} else {
		log.Error("bad sequencer message segment kind", "sequence", r.cachedSegmentNum, "segmentNum", segmentNum, "kind", kind)
		return nil, nil
	}
	return msg, nil
}

func (r *inboxMultiplexer) DelayedMessagesRead() uint64 {
	return r.delayedMessagesRead
}
