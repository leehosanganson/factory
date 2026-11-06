package restjobs

import (
	"strings"
	"testing"
	"time"
)

func TestValidateScheduleCronRequiresFiveFieldsAndMinuteGranularity(t *testing.T) {
	valid := []string{
		"* * * * *",
		"*/1 * * * *",
		"0-59/1 * * * *",
		"5,10,15 * * * *",
		"0 9 * * 1-5",
		"0 0 1 * 0",
		"0 0 * * 7",
	}
	for _, expression := range valid {
		if err := ValidateScheduleCron(expression); err != nil {
			t.Errorf("ValidateScheduleCron(%q) = %v, want nil", expression, err)
		}
	}
	invalid := []string{
		"@hourly",
		"@every 1m",
		"@every 60s",
		strings.Repeat("0,", 65) + "0 * * * *",
		"* * * *",
		"* * * * * *",
		"60 * * * *",
		"* 24 * * *",
		"* * 0 * *",
		"* * * 13 *",
		"*/0 * * * *",
		"1-5/0 * * * *",
		"5-1 * * * *",
		"1,,2 * * * *",
		"x * * * *",
	}
	for _, expression := range invalid {
		if err := ValidateScheduleCron(expression); err == nil {
			t.Errorf("ValidateScheduleCron(%q) = nil, want validation error", expression)
		}
	}
}

func TestNextScheduleOccurrenceIsStrictlyAfterInUTCAndUsesDOMDOWOr(t *testing.T) {
	utc := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) // Wednesday.
	cases := []struct {
		name  string
		expr  string
		after time.Time
		want  time.Time
	}{
		{"next minute", "* * * * *", utc, utc.Add(time.Minute)},
		{"seconds round forward", "* * * * *", utc.Add(30 * time.Second), utc.Add(time.Minute)},
		{"DOM and DOW use OR for DOM", "0 0 1 * 5", time.Date(2024, 12, 31, 0, 1, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"DOM and DOW use OR for DOW", "0 0 1 * 5", time.Date(2025, 1, 2, 0, 1, 0, 0, time.UTC), time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)},
		{"UTC ignores input location", "0 9 * * *", time.Date(2025, 1, 1, 10, 1, 0, 0, time.FixedZone("offset", 3600)), time.Date(2025, 1, 2, 9, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NextScheduleOccurrence(tc.expr, tc.after)
			if err != nil || !got.Equal(tc.want) || got.Location() != time.UTC {
				t.Fatalf("NextScheduleOccurrence(%q, %s) = %s, %v; want %s UTC", tc.expr, tc.after, got, err, tc.want)
			}
		})
	}
}
