package avail

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	gsrpc_types "github.com/centrifuge/go-substrate-rpc-client/v4/types"
	"github.com/ethereum/go-ethereum/common"
)

func TestMarshallingAndUnmarshalingBlobPointer(t *testing.T) {
	extrinsicIndex := 1
	bridgeApiBaseURL := "https://hex-bridge-api.sandbox.avail.tools"
	blockHashPath := "/eth/proof/" + "0xf53613fa06b6b7f9dc5e4cf5f2849affc94e19d8a9e8999207ece01175c988ed" //+ finalizedblockHash.Hex()
	params := url.Values{}
	params.Add("index", fmt.Sprint(extrinsicIndex))

	u, _ := url.ParseRequestURI(bridgeApiBaseURL)
	u.Path = blockHashPath
	u.RawQuery = params.Encode()
	urlStr := fmt.Sprintf("%v", u)
	t.Log(urlStr)
	// TODO: Add time difference between batch submission and querying merkle proof
	resp, err := http.Get(urlStr) //nolint
	if err != nil {
		t.Fatalf("Bridge Api request not successfull, err=%v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("unable to read data from response body, err=%v", err)
	}
	var bridgeApiResponse BridgeApiResponse
	err = json.Unmarshal(body, &bridgeApiResponse)
	if err != nil {
		t.Fatalf("unable to unmarshal bridge api response, err=%v", err)
	}
	t.Logf("%+v", bridgeApiResponse)

	var dataRootProof [][32]byte
	for _, hash := range bridgeApiResponse.DataRootProof {
		var byte32Array [32]byte
		copy(byte32Array[:], hash[:])
		dataRootProof = append(dataRootProof, byte32Array)
	}
	var leafProof [][32]byte
	for _, hash := range bridgeApiResponse.LeafProof {
		var byte32Array [32]byte
		copy(byte32Array[:], hash[:])
		leafProof = append(leafProof, byte32Array)
	}
	var merkleProofInput MerkleProofInput = MerkleProofInput{dataRootProof, leafProof, bridgeApiResponse.RangeHash, bridgeApiResponse.DataRootIndex, bridgeApiResponse.BlobRoot, bridgeApiResponse.BridgeRoot, bridgeApiResponse.Leaf, bridgeApiResponse.LeafIndex}
	t.Logf("%+v", merkleProofInput)

	var blobPointer BlobPointer = BlobPointer{gsrpc_types.NewHash([]byte{245, 54, 19, 250, 6, 182, 183, 249, 220, 94, 76, 245, 242, 132, 154, 255, 201, 78, 25, 216, 169, 232, 153, 146, 7, 236, 224, 17, 117, 201, 136, 237}),
		"5EFLq4DT8M2TpSqU3gYRf38SAn7x8Vsbiuhp72E9Ri3FQxn7",
		100,
		common.HexToHash("0xf53613fa06b6b7f9dc5e4cf5f2849affc94e19d8a9e8999207ece01175c988ed"),
		merkleProofInput,
	}

	data, err := blobPointer.MarshalToBinary()
	if err != nil {
		t.Fatalf("unable to marshal blobPointer to binary, err=%v", err)
	}
	t.Logf("%x", data)

	var newBlobPointer = BlobPointer{}
	if err := newBlobPointer.UnmarshalFromBinary(data[1:]); err != nil {
		t.Fatalf("unable to unmarhal blobPoiter from binary, err=%v", err)
	}

	t.Logf("%+v", newBlobPointer)
}
