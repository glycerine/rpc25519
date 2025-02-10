package bytes

import (
	"testing"
)

func TestSIMDLongestZeroSpan_and_LongestZeroSpan(t *testing.T) {
	tests := []struct {
		name       string
		input      []byte
		wantStart  int
		wantLength int
	}{
		{
			name:       "empty slice",
			input:      []byte{},
			wantStart:  0,
			wantLength: 0,
		},
		{
			name:       "no zeros",
			input:      []byte{1, 2, 3},
			wantStart:  0,
			wantLength: 0,
		},
		{
			name:       "single zero",
			input:      []byte{1, 0, 1},
			wantStart:  1,
			wantLength: 1,
		},
		{
			name:       "all zeros",
			input:      []byte{0, 0, 0},
			wantStart:  0,
			wantLength: 3,
		},
		{
			name:       "multiple spans, first longest",
			input:      []byte{0, 0, 0, 1, 0, 0},
			wantStart:  0,
			wantLength: 3,
		},
		{
			name:       "multiple spans, last longest",
			input:      []byte{0, 0, 1, 0, 0, 0},
			wantStart:  3,
			wantLength: 3,
		},
		{
			name:       "multiple equal spans",
			input:      []byte{0, 0, 1, 0, 0},
			wantStart:  0,
			wantLength: 2,
		},
		{
			name:       "zeros at start",
			input:      []byte{0, 0, 0, 1, 2, 3},
			wantStart:  0,
			wantLength: 3,
		},
		{
			name:       "zeros at end",
			input:      []byte{1, 2, 3, 0, 0, 0},
			wantStart:  3,
			wantLength: 3,
		},
		{
			name:       "large span in middle",
			input:      []byte{1, 0, 0, 0, 0, 1},
			wantStart:  1,
			wantLength: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotLength := SIMDLongestZeroSpan(tt.input)
			if gotStart != tt.wantStart || gotLength != tt.wantLength {
				t.Errorf("SIMDLongestZeroSpan() = (%v, %v), want (%v, %v)",
					gotStart, gotLength, tt.wantStart, tt.wantLength)
			}
		})

		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotLength := LongestZeroSpan(tt.input)
			if gotStart != tt.wantStart || gotLength != tt.wantLength {
				t.Errorf("LongestZeroSpan() = (%v, %v), want (%v, %v)",
					gotStart, gotLength, tt.wantStart, tt.wantLength)
			}
		})

	}
}
