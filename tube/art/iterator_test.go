package art

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var _ = bytes.Compare
var _ = fmt.Sprintf
var _ = strconv.Atoi
var _ = strings.Split

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestIterConcurrentExpansion(t *testing.T) {

	var (
		tree = NewArtTree()
		keys = [][]byte{
			[]byte("aaba"),
			[]byte("aabb"),
		}
	)

	for _, key := range keys {
		tree.Insert(key, key, "")
	}
	//vv("orig tree: %v", tree)
	iter := tree.Iter(nil, nil)
	if !iter.Next() {
		t.Fatal("expected Next() to return true")
	}
	if got, want := iter.Key(), Key(keys[0]); !bytes.Equal(got, want) {
		t.Errorf("got key %v, want %v", got, want)
	}

	// adding a 3rd key, after iter started,
	// that is after the 2nd key we have not read yet.
	tree.Insert([]byte("aaca"), nil, "")

	//vv("after adding 'aaca', tree: %v", tree)

	if !iter.Next() {
		t.Fatal("expected Next() to return true")
	}
	if got, want := iter.Key(), Key(keys[1]); !bytes.Equal(got, want) {
		t.Errorf("got key %v, want %v", string(got), string(want))
	}

	if !iter.Next() {
		t.Fatal("expected Next() to return true")
	}
	if got, want := iter.Key(), Key("aaca"); !bytes.Equal(got, want) {
		t.Errorf("got key %v, want %v", string(got), string(want))
	}
}

func TestIterDeleteBehindFwd(t *testing.T) {

	tree := NewArtTree()
	N := 60000
	for i := range N {
		k := fmt.Sprintf("%09d", i)
		key := Key(k) // []byte
		tree.Insert(key, key, "")
	}
	//vv("full tree before any delete/iter: '%s'", tree)
	got := make(map[int]int)
	deleted := make(map[int]int)
	kept := make(map[int]int)

	iter := tree.Iter(nil, nil)
	thresh := 5000
	for iter.Next() {
		sz := tree.Size()
		k := iter.Key()
		nk, err := strconv.Atoi(strings.TrimSpace(string(k)))
		panicOn(err)
		got[nk] = len(got)
		// e.g. for N=6 and thresh=4 => delete 0,1,2,3. keep 4,5
		if nk < thresh {
			gone, _ := tree.Remove(k)
			if !gone {
				panic("should have gone")
			}
			deleted[nk] = len(deleted)

			sz2 := tree.Size()
			if sz2 != sz-1 {
				//vv("tree now '%s'", tree)
				panic("should have shrunk tree")
			}
		} else {
			kept[nk] = len(kept)
		}
	}
	sz := tree.Size()
	//vv("after iter, sz = %v", sz)
	//vv("got (len %v) = '%#v'", len(got), got)
	//vv("deleted (len %v) = '%#v'", len(deleted), deleted)
	//vv("kept (len %v) = '%#v'", len(kept), kept)

	if thresh > N {
		thresh = N // simpler verification below, no change in above.
	}

	if sz != (N - thresh) {
		t.Fatalf("expected tree to be size %v, but see %v", N-thresh, sz)
	}
	if len(got) != N {
		t.Fatalf("expected got(len %v) to be len %v", len(got), N)
	}
	if len(deleted) != thresh {
		t.Fatalf("expected deleted(len %v) to be len %v",
			len(deleted), thresh)
	}
	//vv("tree at end '%s'", tree)
	for i := thresh; i < N; i++ {
		k := fmt.Sprintf("%09d", i)
		key := Key(k) // []b
		_, _, found, _ := tree.FindExact(key)
		if !found {
			t.Fatalf("expected to find '%v' still in tree", k)
		}
	}

	for i := 0; i < N; i++ {
		if _, ok := got[i]; !ok {
			t.Fatalf("expected got[i=%v] to be present.", i)
		}
		if i < thresh {
			if _, ok := deleted[i]; !ok {
				t.Fatalf("expected deleted[i=%v] to be present.", i)
			}
		} else {
			if _, ok := kept[i]; !ok {
				t.Fatalf("expected kept[i=%v] to be present.", i)
			}
		}
	}
}

func TestIterDeleteBehindReverse(t *testing.T) {

	tree := NewArtTree()
	N := 60_000
	if N >= 1_000_000_000 {
		panic(`must bump up the Sprintf("%09d", i) ` +
			`have sufficient lead 0 padding`)
	}
	for i := range N {
		// if we don't zero pad, then lexicographic
		// delete order is very different from
		// numerical order, and we might get
		// confused below--like we did at first
		// when wondering why 8 and 9 are the
		// first two deletions with 60 keys
		// in the tree. Lexicographically, they
		// are the largest.
		k := fmt.Sprintf("%09d", i)
		key := Key(k) // []byte
		tree.Insert(key, key, "")
	}
	//vv("full tree before any delete/iter: '%s'", tree)
	got := make(map[int]int)
	deleted := make(map[int]int)
	kept := make(map[int]int)

	iter := tree.RevIter(nil, nil)

	thresh := 20_000
	callcount := 0
	for iter.Next() {
		callcount++
		sz := tree.Size()
		k := iter.Key()
		nk, err := strconv.Atoi(strings.TrimSpace(string(k)))
		panicOn(err)
		got[nk] = len(got)
		// reversed testing uses callcount here,
		// so that reversed (order issued) actually matters.
		// e.g. for N=6, iter should return     5,4,3,2,1,0
		// and so for thresh = 2, we should del 5,4         (len thresh)
		//                         and keep         3,2,1,0 (len N-thresh)
		// kept is < N-thresh;
		// deleted is >= N-thresh
		if callcount <= thresh {
			//vv("calling Remove(%v)", nk)
			gone, _ := tree.Remove(k)
			if !gone {
				panic("should have gone")
			}
			deleted[nk] = len(deleted)

			sz2 := tree.Size()
			if sz2 != sz-1 {
				//vv("tree now '%s'", tree)
				panic("should have shrunk tree")
			}
		} else {
			kept[nk] = len(kept)
		}
	}
	sz := tree.Size()
	//vv("after iter, sz = %v", sz)
	//vv("got (len %v) = '%#v'", len(got), got)
	//vv("deleted (len %v) = '%#v'", len(deleted), deleted)
	//vv("kept (len %v) = '%#v'", len(kept), kept)

	if thresh > N {
		thresh = N // simpler verification below
	}

	if sz != (N - thresh) {
		t.Fatalf("expected tree to be size %v, but see %v", N-thresh, sz)
	}
	if len(got) != N {
		t.Fatalf("expected got(len %v) to be len %v", len(got), N)
	}
	if len(deleted) != thresh {
		t.Fatalf("expected deleted(len %v) to be len %v",
			len(deleted), thresh)
	}
	//vv("tree at end '%s'", tree)
	// kept: i < N-thresh
	for i := 0; i < N-thresh; i++ {
		k := fmt.Sprintf("%09d", i)
		key := Key(k) // []b
		_, _, found, _ := tree.FindExact(key)
		if !found {
			t.Fatalf("expected to find '%v' still in tree", k)
		}
	}

	for i := 0; i < N; i++ {
		if _, ok := got[i]; !ok {
			t.Fatalf("expected got[i=%v] to be present.", i)
		}
		if i >= N-thresh {
			if _, ok := deleted[i]; !ok {
				t.Fatalf("expected deleted[i=%v] to be present.", i)
			}
		} else {
			if _, ok := kept[i]; !ok {
				t.Fatalf("expected kept[i=%v] to be present.", i)
			}
		}
	}
}

// [start, end) semantics version; not (start, end].
func TestIterator(t *testing.T) {

	keys := []string{
		"1234",
		"1245",
		"1267",
		"1345",
	}
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	reversed := make([]string, len(keys))
	copy(reversed, keys)
	sort.Sort(sort.Reverse(sort.StringSlice(reversed)))

	for _, tc := range []struct {
		desc       string
		keys       []string
		start, end string
		reverse    bool
		want       []string
	}{
		{
			desc: "full",
			keys: keys,
			want: sorted,
		},
		{
			desc: "empty",
			want: []string{},
		},
		{
			desc: "matching leaf",
			keys: keys[:1],
			want: keys[:1],
		},
		{
			desc:  "non matching leaf",
			keys:  keys[:1],
			want:  []string{},
			start: "13",
		},
		{
			desc: "limited by end",
			keys: keys,
			end:  "125",
			want: sorted[:2],
		},
		{
			desc:  "limited by start",
			keys:  keys,
			start: "124",
			want:  sorted[1:],
		},
		{
			desc: "end is excluded",
			keys: keys,
			end:  "1345",
			want: sorted[:3],
		},
		{
			desc:  "start to end",
			keys:  keys,
			start: "125",
			end:   "1345",
			want:  sorted[2:3],
		},
		{
			desc:    "reverse",
			keys:    keys,
			want:    reversed,
			reverse: true,
		},
		{
			desc:    "reverse until",
			keys:    keys,
			end:     "1200",
			want:    reversed,
			reverse: true,
		},
		{
			desc:    "reverse from3",
			keys:    keys,
			start:   "1268",
			want:    reversed[1:],
			reverse: true,
		},
		{
			desc:    "reverse from until",
			keys:    keys,
			end:     "1235",
			start:   "1268",
			want:    reversed[1:3],
			reverse: true,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			tree := NewArtTree()
			for _, key := range tc.keys {
				tree.Insert([]byte(key), []byte(key), "")
			}
			//vv("tree = '%v'", tree)
			var iter Iterator
			if tc.reverse {
				//vv("reverse is true")
				iter = tree.RevIter([]byte(tc.end), []byte(tc.start))
				//vv("iter.reverse is %v", iter.reverse)
			} else {
				iter = tree.Iter([]byte(tc.start), []byte(tc.end))
			}
			want := []string{}
			for iter.Next() {
				want = append(want, string(iter.Value()))
			}
			if !equalStringSlice(want, tc.want) {
				t.Fatalf("got='%v'; want '%v'", want, tc.want)
			}
		})
	}
}

func TestIterRange(t *testing.T) {

	tree := NewArtTree()
	N := 3

	// pick out the extremes to specify the range.
	var first, last []byte

	for i := range N {
		k := fmt.Sprintf("%09d", i)
		key := Key(k) // []byte
		if i == 0 {
			first = append([]byte{}, []byte(key)...)
		}
		if i == N-1 {
			last = append([]byte{}, []byte(key)...)
		}
		tree.Insert(key, key, "")
	}
	//vv("tree: '%s'", tree)
	//vv("first = '%v'", string(first)) // 0
	//vv("last = '%v'", string(last)) // 2
	if true {
		expect := []int{0, 1}
		iter := tree.Iter(first, last) // [0, 2) so 0, 1
		n := 0
		for iter.Next() {
			key := iter.Key()
			k, err := strconv.Atoi(strings.TrimSpace(string(key)))
			panicOn(err)
			//fmt.Printf("item %v was key '%v'\n", n, string(key))
			if k != expect[n] {
				t.Fatalf("want %v, got %v", n, k)
			}
			n++
		}
	}

	if true {
		expect := []int{2, 1}
		riter := tree.RevIter(first, last) // (0,2], so 2, 1
		n := 0
		for riter.Next() {
			key := riter.Key()
			k, err := strconv.Atoi(strings.TrimSpace(string(key)))
			panicOn(err)
			//fmt.Printf("riter item %v was key '%v' -> k=%v; expect: '%v'\n", n, string(key), k, expect[n])
			if k != expect[n] {
				t.Fatalf("want %v, got %v", n, k)
			}
			n++
		}
	}
}

func TestAscendAndDescendIteration(t *testing.T) {

	keys := []string{
		"member_-1edUtuyHCPBstGobzJE",
		"member_-78B0K04ju0mfbX1MWww",
		"member_-SK5-j0ts4TZ2Fdelq07",
		"member_-eoXzbpz4qW0Ag8U3iNU",
		"member_-sReAjJ-7SiiTuFegyGl",
		"member_-t-DFBVVn1JfnaE9Wy5Y",
		"member_-tWIrOIRW_K9nKMJ1uk5",
		"member_0MTkiems5W3X8y9R0B8_",
		"member_2FWEydbAjl7xhvLTEZPG",
		"member_2SyZXJJsmtagaWHXqzlE",
		"member_2WwNPC3x57Vn_y8CT7sn",
		"member_2egkGSbmDD7utBHjtALy",
		"member_38cuzVH2qCNohKi_i4UI",
		"member_4J8GA4BKESllXNH9oKe5",
		"member_4vgH-iStQh29oYdCHwz8",
		"member_55BFGyQSf7h6_88eeb6h",
		"member_5bJZ9CTuJaGTcLPF-PVD",
		"member_6RIb5ClaFCleukwcEZQU",
		"member_6XpqFHtUxzm4B4-hEZnK",
		"member_6hZ4p_mEGHAw-5aKgYzW",
		"member_72rFliFz-HGwMPd-s_VK",
		"member_7Bqg4115p4yqvIElLqyp",
		"member_7KTMMQ8c-QLJCSde5tpU",
		"member_7XspgUCpj78c3ywMGO42",
		"member_7l_rN0fF3dFbqk8V-PTi",
		"member_8DmNpkxoVOTl4dvQTy5H",
		"member_8EKxMufYo9zc5ZRz3E7Y",
		"member_8HqWr5EGKEy1CkLF9mId",
		"member_8tKDEfr2UJKqv1gqZ6ro",
		"member_90D2nAh4adbMFn7OxKoZ",
		"member_9EYH_9dj0-KG9fGZJlKF",
		"member_ADp1aW32BR_8nR6ewiLp",
		"member_Bbc8AkllaB6AVxAEdMhU",
		"member_Bkf3Tu6-kc-AT5imRGW_",
		"member_BmI-LWW3QLJK70vXouUq",
		"member_CDCKqm8zv2mFsR5aeAyK",
		"member_CdbUCGO14gtYJn84C452",
		"member_D1xU71zU0cWn3JWnCezE",
		"member_DAHFy2SLrosPlopN0pQ4",
		"member_DeNFNDuWaYzpbdgsZfQX",
		"member_E4-d5yJIgTARYjm9CJYA",
		"member_EGY4EQgeG2ec1PaO-Btl",
		"member_EhFchUQqCp9THcPqREG4",
		"member_ElbrebncRDsRdof5qF9A",
		"member_FP4cdIASHGWwbxY71E2d",
		"member_FSshWWO3AZyKgYBLT5Fn",
		"member_Fd6aZsuyG10a3pjWL8lf",
		"member_FmmJK86obl463bTDZRLP",
		"member_FnfCRtChYWzz7RUQggr7",
		"member_GRQalnSZYqof_-NSuiS_",
		"member_G_kvlPE3s-E-6lcQ8zOb",
		"member_GsJkZY5-8iTSDa6Sli7A",
		"member_I0cX7lb2YXFYZlKRMr0S",
		"member_I6PSJumDlIHc2YfjFpeT",
		"member_IiIFtnIsE_6pbVNEk6eR",
		"member_IpSL_iztRepHvJK8kf-f",
		"member_J-JqVEDwPIuPH8YxF39o",
		"member_JIIqgEdudxfsJJJYC-AT",
		"member_JVLPn72l81Tgvj_bbQr-",
		"member_JkauYfTt_Dyh7WXqSxFL",
		"member_K6fLt03j4x0IycjdKO0v",
		"member_KizjpLRvRmiG8mLecg1Y",
		"member_LCVdKp3ytU6fXvgqU6xP",
		"member_LhZ7KTLBpIQeo1EWRcdO",
		"member_LtLxzeJc7rUSYJY-VJ2_",
		"member_Lw9C2Y3ikkK7JI9TemSz",
		"member_MNsnG4m7Fuz1adSnrIcB",
		"member_MbPOwZlB9oNSg3qzuf6B",
		"member_N01rCCEXSw_KG0QU3N8G",
		"member_NF5OxcDZ0vlbZ4YGykVC",
		"member_NPLit5Z2wzeSaGsFWxWK",
		"member_NcUmbD-fQEwTiWQykqAO",
		"member_NxVXntKA91ElhfhnnM_E",
		"member_O0_tPvHtO4Cuk3Z5JgG1",
		"member_OzEeDnQFN3RojMINpxWB",
		"member_Plf0yTo82u88QlkubH1W",
		"member_QCpQtGFHlc0Uoj5MghpV",
		"member_QUPv6ur3AFKkdVPHW8sx",
		"member_Qbpc4whH-7PJYMGc6mUj",
		"member_Qcq17rNRMQJF9oxvuhC8",
		"member_QvUGKzSvy8wVgo2nguDO",
		"member_R5h4IzZWV9I6XajYNO5C",
		"member_RVuFJ-HEAAKN2ETe6KcE",
		"member_RxR_PEf0nuP97ZfnUsDT",
		"member_SbSXq8prViK20xTkZgEO",
		"member_SkrxWpNLL0Mn0oPcB5WT",
		"member_StthvBChJ8vdcbTxzK6J",
		"member_Sysei-f3_KvW3JsMMBRh",
		"member_T-XvfT0PSIjD2T6sW91F",
		"member_TVbndndHOdufqPtehl4K",
		"member_To1O4kVnFJF4HBFwnMHJ",
		"member_Tpds1AldT5fwSNDrqd54",
		"member_TwU6hmvUWlOWRFy8rAc5",
		"member_Ty7Ahzc0llL1YnkEiRRE",
		"member_U7JOtEW5p6APkU4qQTdJ",
		"member_UPR2SuvLXWTlo6g95732",
		"member_Ucfp9aFakxIT89fl5jYe",
		"member_Uj55Cquj3AU96Z95FnYx",
		"member_UnlEsp5Mb4KAj5FJAwR_",
		"member_Ur8slMUBw6tsxCfcez62",
		"member_UyRmM-39jSsc-MxlrXpb",
		"member_VnIKPhevsKyygig4f2yS",
		"member_VtEi39xd6NY87TbYeRNB",
		"member_W3-o92y7GfccN753IH_5",
		"member_WT29qSZmSlr2DhMOC_40",
		"member_Wwi1m-UL7PvP9aJrkfsQ",
		"member_X6OgCvDWl9-KQ-4rQ82c",
		"member_X8lwNa75E7OL4nLhGM1f",
		"member_XKraESK4EPyRsw1OkLaV",
		"member_XPxL5SRIh-214ck7N1CJ",
		"member_XfW2dZancVWYFzMBQtuC",
		"member_XhWusEtWFu2IaGOeMop0",
		"member_Y0JjzolYVERc_Bm_4TLD",
		"member_Y43zC9YYBgXUhgtL7cOJ",
		"member_Yu2-I7K9yOhnT2P4bZpc",
		"member_Zr8UYFCX6glaewo8rbdQ",
		"member__05FYEJ-JMmdQhB4-eRX",
		"member__B6GczshV-4r083k1e_f",
		"member__ZeJEK97ymiWAJT1PrBB",
		"member__uWVu8q0Fi2U44aiUBtw",
		"member_aXlv67hbrYkckbk9R8Bj",
		"member_axwBFZReA04DcQmxNm5t",
		"member_bCCAkjzPPIW2HfNyXum9",
		"member_bg7e5PoWPUi3z1FOSkKZ",
		"member_c9ajEUEv2cOAddxs23Vf",
		"member_cifEsF8b9nLYjuV1PUPk",
		"member_ckq70tO72NKyQWn1iksy",
		"member_cpREQgx_cUF5ksQPjnWj",
		"member_d8OaXAHm0ZQ7i3SbOGhU",
		"member_dBQuTIMQ-6AWQUgpoyWa",
		"member_f2u3oE3VhoP4psoXOGKj",
		"member_fS13j6XvB6LY8TML2kqH",
		"member_fWo3GtXRpg6IVnICz7hm",
		"member_facuULue1k4xxXZKLKm3",
		"member_faoz0vZMdGCHclCroCYI",
		"member_fcxwtJDpKmo-yv0gEDyz",
		"member_gXsR6s6Y88e7OKQXHDtz",
		"member_goRk-7MxsWLGrrwMHKJ5",
		"member_gzhKu8jUq7FJm_dl2ZYn",
		"member_hwSPyrL13szh5yfZSotJ",
		"member_i6fH7pY1VxjSLgp_4ul4",
		"member_i_JRkAFo442RXXd5zc1Y",
		"member_isqyWZK83UaVHIz0VcRy",
		"member_jRCsNzYuc9b9WSCYMlWQ",
		"member_jzaEjlQ2v2mCIefezfFG",
		"member_kLrMLP8VHyO7yTqw_En5",
		"member_kQ-wqdVAuIm-9w7jyFuF",
		"member_kQlysfBVnIEL0oKZw_Je",
		"member_ke3yvZpfDtVnAd-2U9kq",
		"member_kg8UiJbOyvzPB5WlKKy-",
		"member_kunHuTkj7Uo72mSPh2Ur",
		"member_l1q91VzUBCz1zT_NSjs6",
		"member_lzOUy5_9Lr7HRQIRcYAP",
		"member_mJs70ZlLg5WS1SSsKPUP",
		"member_mKzuZFUgE6g0hxQkPFxD",
		"member_mPutvECZXHOlGTAiHjS2",
		"member_mRxxr84BKkXtyHcB-ocN",
		"member_mTpwj1yxyyuV8FYm0Q4b",
		"member_mVS-MXk0RSWuj1BgfEfb",
		"member_mZKGy-6UhMSyRFY_du0A",
		"member_nKyon5DmKmKNjuFOF3gc", // keys[160]
		// minimal sized set to see the
		// original error error: the 161 above this comment.

		//_ = []string{ // additional 39 in the original issue, may not be needed.
		"member_nb5lg-7HsGd51R0a8SUZ",
		"member_nkVDsEltf7AVS-rTvuEI",
		"member_nrHgzG1UA7zZ5E1QLkFk",
		"member_o2RFgj0Z345AaaqDqylo",
		"member_o2eqyJFlMTnYpk3yE_yK",
		"member_o2vBw9e_XpDVrwwad3PU",
		"member_oDiKkND-9u81FIW47Z1M",
		"member_oNF_nTHvRSGWwhchwUUz",
		"member_pHLZcn9SGU1W_FondSMb",
		"member_pJ5ooOEuiOxxt9GYvau0",
		"member_pPIFq3_STnPvQCxzcHq7",
		"member_pSYhrUasdULUxCpxBVIL",
		"member_pSq3MAXNN6Iwp7UwNjoc",
		"member_p_MjuQl4mrp9UvAZYxRp",
		"member_pa3X12fMrqaSZs0rki-h",
		"member_pge4E3sij8P3dMFzyjqY",
		"member_qBb5PsHMAS3vqrMCvzbG",
		"member_qumhRU9wwZGOXrzYa4QZ",
		"member_rI8hOpDDYumiolTw_plI",
		"member_reiOfly8tqilNxIXKQ1U",
		"member_s2CKQ6Ei4d8TwaycTquC",
		"member_s7wmb7JAiKdtGZsVaP7C",
		"member_sHSRTa5LA2SHFUOi8b0x",
		"member_tMnQvd6Wp-vwzfRIDJ6q",
		"member_tbc-QLYJoKiFFQ2-8Q7M",
		"member_uMw-67HIjdrAasGFTO-3",
		"member_ukWHNi_slSBNj_XxC-Pv",
		"member_ulmTRdIxjjTvbaJy3ltk",
		"member_vEIGSLWMUoMXVF1F26Jc",
		"member_vQVVHRQaY8J0_Z1WSgb_",
		"member_vU4b_nFTDYleph1rd2_6",
		"member_w5EvaWlMtU4i7Ecz3xDc",
		"member_wFuKcf369iaNAt_oMmtf",
		"member_wNBwJMk_7rvDwkoEYzlM",
		"member_wq24RomNJKpcqWYVrlAA",
		"member_xQVRnQleoBZzR7uzkLQp",
		"member_y9dKqmcXU_Einyshtl_r",
		"member_zbfuZzyzwTJVgDAgfhkh",
		"member_zy5jl8MjvhXSj9a4yK8a",
	}

	keys = keys[:18]

	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	reversed := make([]string, len(keys))
	copy(reversed, keys)
	sort.Sort(sort.Reverse(sort.StringSlice(reversed)))

	//vv("forward sorted = '%#v'", sorted)

	//vv("reversed = '%#v'", reversed)

	tree := NewArtTree()
	for _, key := range keys {
		tree.Insert([]byte(key), nil, "")
	}
	//vv("tree = '%v'", tree)

	i := 0
	if true {
		// check Ascend
		for key, lf := range Ascend(tree, nil, nil) {
			_ = lf
			skey := string(key)
			if skey != keys[i] {
				panicf("Ascend i=%v problem, want '%v', got '%v'", i, keys[i], skey)
			}
			i++
		}
		if i != len(keys) {
			panicf("wanted %v keys back from Ascend, got %v", len(keys), i)
		}
		//vv("%v: Ascend worked fine", t.Name())

		// verify that integer indexing works.
		i = 0
		for j := len(keys) - 1; j >= 0; j-- {
			lf, ok := tree.At(j)
			if !ok {
				break
			}
			skey := string(lf.Key)
			if skey != reversed[i] {
				panicf("At indexing in reverse j=%v problem, want '%v', got '%v'", j, reversed[i], skey)
			}
			//vv("good, observed skey='%v' and expected '%v'", skey, reversed[i])
			i++
		}
		//vv("%v At indexing in reverse was okay.", t.Name())

		i = 0
		for key, lf := range Descend(tree, nil, nil) {
			_ = lf
			skey := string(key)
			if skey != reversed[i] {
				panicf("Descend i=%v problem, want '%v', got '%v'", i, reversed[i], skey)
			}
			i++
		}
		if i != len(keys) {
			panicf("wanted %v keys back from Descend, got %v", len(keys), i)
		}
		//vv("%v: Descend worked fine", t.Name())
	}
	// verify delete in the middle of iteration works in reverse.
	i = 0
	for key, lf := range Descend(tree, nil, nil) {
		_ = lf
		skey := string(key)
		if skey != reversed[i] {
			panicf("Descend with Delete: i=%v problem, want '%v', got '%v'. tree.Len=%v", i, reversed[i], skey, tree.Size())
		}
		i++
		// remove the odd ones as we go.
		if i%2 == 1 {
			//vv("i=%v before removal, tree = '%v'", i, tree)
			tree.Remove(Key(skey))
			//vv("i=%v, removed '%v'", i, skey)
			//vv("i=%v after removal, tree = '%v'", i, tree)
		}
	}
	if i != len(keys) {
		panicf("wanted %v keys back from Descend, got %v", len(keys), i)
	}
}

func TestFindAndAtInverses(t *testing.T) {

	keys := []string{
		"member_-1edUtuyHCPBstGobzJE",
		"member_-78B0K04ju0mfbX1MWww",
		"member_-SK5-j0ts4TZ2Fdelq07",
		"member_-eoXzbpz4qW0Ag8U3iNU",
		"member_-sReAjJ-7SiiTuFegyGl",
		"member_-t-DFBVVn1JfnaE9Wy5Y",
		"member_-tWIrOIRW_K9nKMJ1uk5",
		"member_0MTkiems5W3X8y9R0B8_",
		"member_2FWEydbAjl7xhvLTEZPG",
		"member_2SyZXJJsmtagaWHXqzlE",
		"member_2WwNPC3x57Vn_y8CT7sn",
		"member_2egkGSbmDD7utBHjtALy",
		"member_38cuzVH2qCNohKi_i4UI",
		"member_4J8GA4BKESllXNH9oKe5",
		"member_4vgH-iStQh29oYdCHwz8",
		"member_55BFGyQSf7h6_88eeb6h",
		"member_5bJZ9CTuJaGTcLPF-PVD",
		"member_6RIb5ClaFCleukwcEZQU",
	}

	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	reversed := make([]string, len(keys))
	copy(reversed, keys)
	sort.Sort(sort.Reverse(sort.StringSlice(reversed)))

	//vv("forward sorted = '%#v'", sorted)

	//vv("reversed = '%#v'", reversed)

	tree := NewArtTree()
	for _, key := range keys {
		tree.Insert([]byte(key), nil, "")
	}
	//vv("tree = '%v'", tree)

	n := tree.Size()
	last := n - 1

	for k := 1; k < tree.Size()-1; k++ {
		target := keys[tree.Size()-k]
		tree.Remove(Key(target))
		//vv("after removing '%v', tree='%v'", target, tree)

		n = tree.Size()
		last = n - 1
		for i := range last {
			j := last - i
			skey := keys[j]
			lf, idx, ok := tree.find_unlocked(LT, Key(skey))
			if !ok {
				panicf("j=%v, i=%v (key='%v') not found in tree, but we put it in already; tree='%v'", j, i, skey, tree)
			}
			lfkey0 := string(lf.Key)

			// at() should invert back to what we just received
			lf2, ok2 := tree.at_unlocked(idx)
			if !ok2 {
				panicf("why not ok2?")
			}
			lfkey2 := string(lf2.Key)
			if lfkey2 != lfkey0 {
				panicf("find LT (skey='%v') gave back idx and leaf that do not correspond! idx='%v'; leaf='%v'; but at(idx=%v) -> lf2='%v'", skey, idx, lfkey0, idx, lfkey2)
			}
		}
	}
}

func Test_2nd_AscendAndDescendIteration(t *testing.T) {
	// bug in code that prompted this test.
	// diff --git a/tube/art/gte.go b/tube/art/gte.go
	//
	// index 74444249..a30c03b9 100644
	// --- a/tube/art/gte.go
	// +++ b/tube/art/gte.go
	// @@ -326,7 +326,7 @@ func (n *inner) getGTE(
	//  		if smallestWillDo {
	//  			dir = 2
	//  		}
	// -		return value2, false, dir, 0
	// +		return value2, false, dir, id2
	//
	//  	} // end if dir > 0
	//
	// @@ -385,7 +385,7 @@ func (n *inner) getGTE(
	//  	if dir2 > 0 && smallestWillDo {
	//  		dir2 = 2
	//  	}
	// -	return value2, false, dir2, 0
	// +	return value2, false, dir2, id2
	//  }
	//
	//  // byteCmp helps us track how the key
	// diff --git a/tube/art/lte.go b/tube/art/lte.go
	// index c688ea5f..1df18cca 100644
	// --- a/tube/art/lte.go
	// +++ b/tube/art/lte.go
	// @@ -331,7 +331,7 @@ func (n *inner) getLTE(
	//  		if largestWillDo {
	//  			dir = -2
	//  		}
	// -		return value2, false, dir, 0
	// +		return value2, false, dir, id2
	//
	//  	} // end if dir < 0
	//
	// @@ -391,5 +391,5 @@ func (n *inner) getLTE(
	//  	if dir2 < 0 && largestWillDo {
	//  		dir2 = -2
	//  	}
	// -	return value2, false, dir2, 0
	// +	return value2, false, dir2, id2
	//  }
	// diff --git a/tube/art/n256.go b/tube/art/n256.go
	// index 6831a584..06ebc816 100644
	// --- a/tube/art/n256.go
	// +++ b/tube/art/n256.go
	// @@ -104,7 +104,7 @@ func (n *node256) gt(k *byte) (keyb byte, ch *bnode) {
	//  }
	//
	//  func (n *node256) prev(k *byte) (byte, *bnode) {
	// -	for idx := n.lth - 1; idx >= 0; idx-- {
	// +	for idx := 255; idx >= 0; idx-- {
	//  		b := byte(idx)
	//  		child := n.children[idx]
	//  		if (k == nil || b < *k) && child != nil {

	keys := []string{
		"member_-1tmNqfte9kLpdMlap2g",
		"member_-E865hVhr_OG9q4l5MiO",
		"member_-VF-jGpvoKZTSh4OK-tX",
		"member_-XpcBIFC8n02kh5X7ZT1",
		"member_078XlQytTVSn9aXWkR0N",
		"member_0HxDXipTQgxg-I-3OL-g",
		"member_0VFXK5knCli1HvBHXTvR",
		"member_11W697loil_TB9LecFHY",
		"member_14i35kPoWGEjMjN8KFSG",
		"member_3CItxZzqHZFC4I_y2NBn",
		"member_3kncddt1T3hZpU1jX1yq",
		"member_4KBup9Uf8QZB-cdFkErJ",
		"member_4gUo8M-NfczCE3_WMtLb",
		"member_5Bv0TVzI9gkBM6G8FjT-",
		"member_6BtT_Y1H7oDNj_dIDERU",
		"member_6Kem2lKZkgA9ciNpzo1g",
		"member_6g4yWOmzynoJb_6LXKER",
		"member_6odx68Rz7TMoQyE4q5KU",
		"member_6tE2FNm6Irdjh1eza_7A",
		"member_799EW267i63SUn3ewV_7",
		"member_7aKSQiREFCrv0E_2u7pw",
		"member_8Di5AnlCPCB0wCITi3ng",
		"member_8gxpg4dq5wYNPhpqKrAU",
		"member_90vj83xHB4eWGl_8-fKa",
		"member_9K4pEaeMCmj0Sez4OCrD",
		"member_9L0GyZ6lEUHCBUaSEiMe",
		"member_A2QF1fxNGmeDfVJuxEBc",
		"member_ArQwrHY4eu23FQFLflEi",
		"member_B2cjlsQ1HlC-HtpvJ9nC",
		"member_Bd0P77AxJbZFtX7uC_b9",
		"member_C6qrZJJXn3_HjT4Ymg6I",
		"member_Cy_MMCCp-eG0PgsQnipf",
		"member_DZZI8-f6ZNS40BIVeJ6R",
		"member_ECUGyYz3MglLgBBcVqEM",
		"member_FwbavczTK4zud_xOj3XI",
		"member_GKPmYfzbwSgu_mwQG_Oc",
		"member_GrUBiRiub6d0xHvpF1ec",
		"member_Iev5NwJ1qcMKBoULUEAl",
		"member_JUUqAmZnMqmxkwEvXd93",
		"member_KVApiSEW1Crzruk3VUXy",
		"member_Kxxh-TBrnQCGfizaFys_",
		"member_MZL1CbnlWVk9_sKDeERd",
		"member_NOR88w-KmWiitnRHA1T2",
		"member_NVpRzaRkPNvP5-rW05uM",
		"member_NvcyVIL1GjR1h9b5fF1D",
		"member_OBVzeKek7KPoaZEIKUP8",
		"member_ODlgu2v_XWuNa79wnmZu",
		"member_PcnuICK2gFE5YD_cIb8E",
		"member_QeQ3CZHZ_kZ8fXLXh-za",
		"member_QzsPhxpTXs9d1boMYmY5",
		"member_R8edOxB36Pg12G9sOVFu",
		"member_SGnhWaVstE5XfuOO1dfJ",
		"member_SLlMWF11pM-jbS_3uGSw",
		"member_Sh2fb-8TpatJurJIPrhx",
		"member_StTCZnhINURfr9sFd99h",
		"member_TfqIKmntnb5kbtrJc_8A",
		"member_Tqjm8jjutkYk-9Dah9-O",
		"member_UrdmLZMg3F6sZQ3hOeR8",
		"member_X5bOazSB1hhAf69lvYGi",
		"member_XHsHC4EF2RExf0oaueiM",
		"member_XcoSuIftm72E_tmM6n9D",
		"member_YKkZNc9P2H5dub4D1sF-",
		"member_ZubFIxjP96BpBOEWx0A8",
		"member__V_k4hpgaSFIvDeyuv9t",
		"member_bVnnSzz4Jdq50jyU4_yp",
		"member_dSq0b8xP131H0XQ5wxEz",
		"member_deS64hNbK6r08zrwlAej",
		"member_dwB1njjJNU2472nnr_Id",
		"member_eA0IbOube4UkVWxXxlda",
		"member_fX5RUAmSStMCwlT0exIQ",
		"member_hPUf031ep21xCvmlDgTZ",
		"member_jE9DrzJqKT8iusIeKXhK",
		"member_k1PTW2bEnyioXP99sTYn",
		"member_l0y7k_Ar-0uFGZKeE8eN",
		"member_lXoCTYpwFTSZ0u-0eR_g",
		"member_mpGmnnU5lTzchuMCkLed",
		"member_muMPvg7vp47oqQNLLRCe",
		"member_mzNlTfl5rz98kQrJQL6N",
		"member_nV4VLcoH29fclRUVeKPj",
		"member_nZSZyJ57NiXupeplNXJz",
		"member_nwsUbVw1jdfG6FMtGi5x",
		"member_oOlxIXA1l3n6kliMjfTi",
		"member_omVS8yKlFpCmiUr9jPUf",
		"member_ovZZ4XjFpCPzGeF7oS9T",
		"member_pSuXJ3Om6PyoH0HdKFhZ",
		"member_qWb1qmFw3lE2bGHQHNdt",
		"member_qjkYBgrwEu7coxroxBLZ",
		"member_r9Y7L7l211klkazoGWFv",
		"member_sG_5xrgccjJ1OFusXIqS",
		"member_sdLAaEY1aK0SMggtevWv",
		"member_txyfrx-x0LQ1AJqNyi3j",
		"member_tzndrdBN2Q9UJfSZ3LN8",
		"member_uKaLzHejEDTh-u2as0Ck",
		"member_uSxEqVl-Zzoo_qUVwvf1",
		"member_wAohUtmpPXhCchN20EeP",
		"member_wQX4zD4102pGlOpUpCUM",
		"member_wY7Ox0EsXHuljOdVTXFl",
		"member_xvmSj5dft_DZ2EPhnE6_",
		"member_ymzCnWWv1kJJm8-xaTmn",
	}

	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	reversed := make([]string, len(keys))
	copy(reversed, keys)
	sort.Sort(sort.Reverse(sort.StringSlice(reversed)))

	//vv("forward sorted = '%#v'", sorted)

	//vv("reversed = '%#v'", reversed)

	tree := NewArtTree()
	for _, key := range keys {
		tree.Insert([]byte(key), nil, "")
	}
	//vv("tree = '%v'", tree)

	i := 0
	if true {
		// check Ascend
		for key, lf := range Ascend(tree, nil, nil) {
			_ = lf
			skey := string(key)
			if skey != keys[i] {
				panicf("Ascend i=%v problem, want '%v', got '%v'", i, keys[i], skey)
			}
			i++
		}
		if i != len(keys) {
			panicf("wanted %v keys back from Ascend, got %v", len(keys), i)
		}
		//vv("%v: Ascend worked fine", t.Name())

		// verify that integer indexing works.
		i = 0
		for j := len(keys) - 1; j >= 0; j-- {
			lf, ok := tree.At(j)
			if !ok {
				break
			}
			skey := string(lf.Key)
			if skey != reversed[i] {
				panicf("At indexing in reverse j=%v problem, want '%v', got '%v'", j, reversed[i], skey)
			}
			//vv("good, observed skey='%v' and expected '%v'", skey, reversed[i])
			i++
		}
		//vv("%v At indexing in reverse was okay.", t.Name())

		i = 0
		for key, lf := range Descend(tree, nil, nil) {
			_ = lf
			skey := string(key)
			if skey != reversed[i] {
				panicf("Descend i=%v problem, want '%v', got '%v'", i, reversed[i], skey)
			}
			i++
		}
		if i != len(keys) {
			panicf("wanted %v keys back from Descend, got %v", len(keys), i)
		}
		//vv("%v: Descend worked fine", t.Name())
	}
	// verify delete in the middle of iteration works in reverse.
	i = 0
	for key, lf := range Descend(tree, nil, nil) {
		_ = lf
		skey := string(key)
		if skey != reversed[i] {
			panicf("Descend with Delete: i=%v problem, want '%v', got '%v'. tree.Len=%v", i, reversed[i], skey, tree.Size())
		}
		i++
		// remove the odd ones as we go.
		if i%2 == 1 {
			//vv("i=%v before removal, tree = '%v'", i, tree)
			tree.Remove(Key(skey))
			//vv("i=%v, removed '%v'", i, skey)
			//vv("i=%v after removal, tree = '%v'", i, tree)
		}
	}
	if i != len(keys) {
		panicf("wanted %v keys back from Descend, got %v", len(keys), i)
	}
}
