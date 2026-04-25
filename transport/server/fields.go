package server

import (
	"go.sriracha.dev/sriracha"
	srirachav1 "go.sriracha.dev/transport/proto/srirachav1"
)

// partitionFields separates requestedFields into held (server supports) and notHeld.
func partitionFields(requestedFields, supportedFields []string) (held, notHeld []string) {
	supported := make(map[string]struct{}, len(supportedFields))
	for _, f := range supportedFields {
		supported[f] = struct{}{}
	}
	for _, f := range requestedFields {
		if _, ok := supported[f]; ok {
			held = append(held, f)
		} else {
			notHeld = append(notHeld, f)
		}
	}
	return held, notHeld
}

// buildFieldValues extracts field values from record for the held fields.
// Fields that are NotFound are collected in notFound; NotHeld values from the
// record are treated as not held for that specific record.
func buildFieldValues(record sriracha.RawRecord, held []string) (fields []*srirachav1.FieldValue, notFound []string) {
	for _, fpStr := range held {
		fp, err := sriracha.ParseFieldPath(fpStr)
		if err != nil {
			notFound = append(notFound, fpStr)
			continue
		}

		val, ok := record[fp]
		switch {
		case !ok || sriracha.IsNotFound(val):
			notFound = append(notFound, fpStr)
		case sriracha.IsNotHeld(val):
			// record-level not-held treated as not-found for the matched record
			notFound = append(notFound, fpStr)
		default:
			fields = append(fields, &srirachav1.FieldValue{
				FieldPath: fpStr,
				Value:     val,
			})
		}
	}
	return fields, notFound
}
