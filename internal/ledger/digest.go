package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
)

func membershipDigest(items []domain.LedgerItem) string {
	ordered := append([]domain.LedgerItem(nil), items...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].WorkloadID == ordered[j].WorkloadID {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].WorkloadID < ordered[j].WorkloadID
	})
	h := sha256.New()
	for _, item := range ordered {
		fmt.Fprintf(h, "%s\x00%s\x00%d\n", item.ID, item.WorkloadID, item.Revision)
	}
	return hex.EncodeToString(h.Sum(nil))
}
