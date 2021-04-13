package libovsdb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAPIListSimple(t *testing.T) {
	cache := apiTestCache(t)
	lscacheList := []Model{
		&testLogicalSwitch{
			UUID:        aUUID0,
			Name:        "ls0",
			ExternalIds: map[string]string{"foo": "bar"},
		},
		&testLogicalSwitch{
			UUID:        aUUID1,
			Name:        "ls1",
			ExternalIds: map[string]string{"foo": "baz"},
		},
		&testLogicalSwitch{
			UUID:        aUUID2,
			Name:        "ls2",
			ExternalIds: map[string]string{"foo": "baz"},
		},
		&testLogicalSwitch{
			UUID:        aUUID3,
			Name:        "ls4",
			ExternalIds: map[string]string{"foo": "baz"},
			Ports:       []string{"port0", "port1"},
		},
	}
	lscache := map[string]Model{}
	for i := range lscacheList {
		lscache[lscacheList[i].(*testLogicalSwitch).UUID] = lscacheList[i]
	}
	cache.cache["Logical_Switch"] = &RowCache{cache: lscache}
	cache.cache["Logical_Switch_Port"] = newRowCache() // empty

	test := []struct {
		name       string
		initialCap int
		resultCap  int
		resultLen  int
		content    []Model
		err        bool
	}{
		{
			name:       "full",
			initialCap: 0,
			resultCap:  len(lscache),
			resultLen:  len(lscacheList),
			content:    lscacheList,
			err:        false,
		},
		{
			name:       "single",
			initialCap: 1,
			resultCap:  1,
			resultLen:  1,
			content:    lscacheList[0:0],
			err:        false,
		},
		{
			name:       "multiple",
			initialCap: 2,
			resultCap:  2,
			resultLen:  2,
			content:    lscacheList[0:2],
			err:        false,
		},
	}
	for _, tt := range test {
		t.Run(fmt.Sprintf("ApiList: %s", tt.name), func(t *testing.T) {
			var result []testLogicalSwitch
			if tt.initialCap != 0 {
				result = make([]testLogicalSwitch, tt.initialCap)
			}
			api := newAPI(cache)
			err := api.List(&result)
			if tt.err {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Lenf(t, result, tt.resultLen, "Length should match expected")
				assert.Equal(t, cap(result), tt.resultCap, "Capability should match expected")
				assert.ElementsMatchf(t, tt.content, tt.content, "Content should match")
			}

		})
	}

	t.Run("ApiList: Error wrong type", func(t *testing.T) {
		var result []string
		api := newAPI(cache)
		err := api.List(&result)
		assert.NotNil(t, err)
	})

	t.Run("ApiList: Type Selection", func(t *testing.T) {
		var result []testLogicalSwitchPort
		api := newAPI(cache)
		err := api.List(&result)
		assert.Nil(t, err)
		assert.Len(t, result, 0, "Should be empty since cache is empty")
	})
}

func TestAPIListPredicate(t *testing.T) {
	cache := apiTestCache(t)
	lscacheList := []Model{
		&testLogicalSwitch{
			UUID:        aUUID0,
			Name:        "ls0",
			ExternalIds: map[string]string{"foo": "bar"},
		},
		&testLogicalSwitch{
			UUID:        aUUID1,
			Name:        "magicLs1",
			ExternalIds: map[string]string{"foo": "baz"},
		},
		&testLogicalSwitch{
			UUID:        aUUID2,
			Name:        "ls2",
			ExternalIds: map[string]string{"foo": "baz"},
		},
		&testLogicalSwitch{
			UUID:        aUUID3,
			Name:        "magicLs2",
			ExternalIds: map[string]string{"foo": "baz"},
			Ports:       []string{"port0", "port1"},
		},
	}
	lscache := map[string]Model{}
	for i := range lscacheList {
		lscache[lscacheList[i].(*testLogicalSwitch).UUID] = lscacheList[i]
	}
	cache.cache["Logical_Switch"] = &RowCache{cache: lscache}

	test := []struct {
		name      string
		predicate interface{}
		content   []Model
		err       bool
	}{
		{
			name: "none",
			predicate: func(t *testLogicalSwitch) bool {
				return false
			},
			content: []Model{},
			err:     false,
		},
		{
			name: "all",
			predicate: func(t *testLogicalSwitch) bool {
				return true
			},
			content: lscacheList,
			err:     false,
		},
		{
			name: "nil function must fail",
			err:  true,
		},
		{
			name: "arbitrary condition",
			predicate: func(t *testLogicalSwitch) bool {
				return strings.HasPrefix(t.Name, "magic")
			},
			content: []Model{lscacheList[1], lscacheList[3]},
			err:     false,
		},
		{
			name: "error wrong type",
			predicate: func(t testLogicalSwitch) string {
				return "foo"
			},
			err: true,
		},
	}

	for _, tt := range test {
		t.Run(fmt.Sprintf("ApiListPredicate: %s", tt.name), func(t *testing.T) {
			var result []testLogicalSwitch
			api := newAPI(cache)
			cond := api.Where(tt.predicate)
			err := cond.List(&result)
			if tt.err {
				assert.NotNil(t, err)
			} else {
				if !assert.Nil(t, err) {
					t.Log(err)
				}
				assert.ElementsMatchf(t, tt.content, tt.content, "Content should match")
			}

		})
	}
}

func TestAPIListFields(t *testing.T) {
	cache := apiTestCache(t)
	lspcacheList := []Model{
		&testLogicalSwitchPort{
			UUID:        aUUID0,
			Name:        "lsp0",
			ExternalIds: map[string]string{"foo": "bar"},
			Enabled:     []bool{true},
		},
		&testLogicalSwitchPort{
			UUID:        aUUID1,
			Name:        "magiclsp1",
			ExternalIds: map[string]string{"foo": "baz"},
			Enabled:     []bool{false},
		},
		&testLogicalSwitchPort{
			UUID:        aUUID2,
			Name:        "lsp2",
			ExternalIds: map[string]string{"unique": "id"},
			Enabled:     []bool{false},
		},
		&testLogicalSwitchPort{
			UUID:        aUUID3,
			Name:        "magiclsp2",
			ExternalIds: map[string]string{"foo": "baz"},
			Enabled:     []bool{true},
		},
	}
	lspcache := map[string]Model{}
	for i := range lspcacheList {
		lspcache[lspcacheList[i].(*testLogicalSwitchPort).UUID] = lspcacheList[i]
	}
	cache.cache["Logical_Switch_Port"] = &RowCache{cache: lspcache}

	testObj := testLogicalSwitchPort{}

	test := []struct {
		name    string
		fields  []interface{}
		prepare func(*testLogicalSwitchPort)
		content []Model
		err     bool
	}{
		{
			name:    "empty object must match everything",
			content: lspcacheList,
			err:     false,
		},
		{
			name: "List unique by UUID",
			prepare: func(t *testLogicalSwitchPort) {
				t.UUID = aUUID0
			},
			content: []Model{lspcache[aUUID0]},
			err:     false,
		},
		{
			name: "List unique by Index",
			prepare: func(t *testLogicalSwitchPort) {
				t.Name = "lsp2"
			},
			content: []Model{lspcache[aUUID2]},
			err:     false,
		},
		{
			name: "List unique by extra field",
			prepare: func(t *testLogicalSwitchPort) {
				t.ExternalIds = map[string]string{"unique": "id"}
			},
			content: []Model{lspcache[aUUID2]},
			fields:  []interface{}{&testObj.ExternalIds},
			err:     false,
		},
		{
			name: "List by extra field",
			prepare: func(t *testLogicalSwitchPort) {
				t.Enabled = []bool{true}
			},
			content: []Model{lspcache[aUUID0], lspcache[aUUID3]},
			fields:  []interface{}{&testObj.Enabled},
			err:     false,
		},
	}

	for _, tt := range test {
		t.Run(fmt.Sprintf("ApiListFields: %s", tt.name), func(t *testing.T) {
			var result []testLogicalSwitchPort
			// Clean object
			testObj = testLogicalSwitchPort{}
			api := newAPI(cache)
			err := api.Where(&testObj, tt.fields...).List(&result)
			if tt.err {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.ElementsMatchf(t, tt.content, tt.content, "Content should match")
			}

		})
	}

	t.Run("ApiListFields: Wrong table", func(t *testing.T) {
		var result []testLogicalSwitchPort
		api := newAPI(cache)
		obj := testLogicalSwitch{
			UUID: aUUID0,
		}

		err := api.Where(&obj).List(&result)
		assert.NotNil(t, err)
	})

	t.Run("ApiListFields: Wrong object field", func(t *testing.T) {
		var result []testLogicalSwitchPort
		api := newAPI(cache)
		obj := testLogicalSwitch{}
		obj2 := testLogicalSwitch{
			UUID: aUUID0,
		}

		err := api.Where(&obj, &obj2.UUID).List(&result)
		assert.NotNil(t, err)
	})
}

func TestWhere(t *testing.T) {
	test := []struct {
		name  string
		arg   interface{}
		extra []interface{}
		err   bool
	}{
		{
			name: "wrong function must fail",
			arg: func(s string) bool {
				return false
			},
			err: true,
		},
		{
			name: "wrong function must fail2 ",
			arg: func(t *testLogicalSwitch) string {
				return "foo"
			},
			err: true,
		},
		{
			name: "wrong model must fail",
			arg:  &struct{ a string }{},
			err:  true,
		},
		{
			name: "correct model should succeed",
			arg:  &testLogicalSwitch{},
			err:  false,
		},
		{
			name: "correct func should succeed",
			arg: func(t *testLogicalSwitch) bool {
				return true
			},
			err: false,
		},
	}

	for _, tt := range test {
		t.Run(fmt.Sprintf("Where: %s", tt.name), func(t *testing.T) {
			cache := apiTestCache(t)
			api := newAPI(cache)
			conditional := api.Where(tt.arg, tt.extra...)
			if tt.err {
				assert.IsType(t, errorApi{}, conditional)
			} else {
				assert.IsType(t, api, conditional)
				t.Logf("%+v", conditional)

			}
		})
	}
}

func TestAPIGet(t *testing.T) {
	cache := apiTestCache(t)
	lsCacheList := []Model{}
	lspCacheList := []Model{
		&testLogicalSwitchPort{
			UUID:        aUUID2,
			Name:        "lsp0",
			Type:        "foo",
			ExternalIds: map[string]string{"foo": "bar"},
		},
		&testLogicalSwitchPort{
			UUID:        aUUID3,
			Name:        "lsp1",
			Type:        "bar",
			ExternalIds: map[string]string{"foo": "baz"},
		},
	}
	lsCache := map[string]Model{}
	lspCache := map[string]Model{}
	for i := range lsCacheList {
		lsCache[lsCacheList[i].(*testLogicalSwitch).UUID] = lsCacheList[i]
	}
	for i := range lspCacheList {
		lspCache[lspCacheList[i].(*testLogicalSwitchPort).UUID] = lspCacheList[i]
	}
	cache.cache["Logical_Switch"] = &RowCache{cache: lsCache}
	cache.cache["Logical_Switch_Port"] = &RowCache{cache: lspCache}

	test := []struct {
		name    string
		prepare func(Model)
		result  Model
		err     bool
	}{
		{
			name: "empty",
			prepare: func(m Model) {
			},
			err: true,
		},
		{
			name: "non_existing",
			prepare: func(m Model) {
				m.(*testLogicalSwitchPort).Name = "foo"
			},
			err: true,
		},
		{
			name: "by UUID",
			prepare: func(m Model) {
				m.(*testLogicalSwitchPort).UUID = aUUID3
			},
			result: lspCacheList[1],
			err:    false,
		},
		{
			name: "by name",
			prepare: func(m Model) {
				m.(*testLogicalSwitchPort).Name = "lsp0"
			},
			result: lspCacheList[0],
			err:    false,
		},
	}
	for _, tt := range test {
		t.Run(fmt.Sprintf("ApiGet: %s", tt.name), func(t *testing.T) {
			var result testLogicalSwitchPort
			tt.prepare(&result)
			api := newAPI(cache)
			err := api.Get(&result)
			if tt.err {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equalf(t, tt.result, &result, "Result should match")
			}
		})
	}
}

func TestAPICreate(t *testing.T) {
	cache := apiTestCache(t)
	lsCacheList := []Model{}
	lspCacheList := []Model{
		&testLogicalSwitchPort{
			UUID:        aUUID2,
			Name:        "lsp0",
			Type:        "foo",
			ExternalIds: map[string]string{"foo": "bar"},
		},
		&testLogicalSwitchPort{
			UUID:        aUUID3,
			Name:        "lsp1",
			Type:        "bar",
			ExternalIds: map[string]string{"foo": "baz"},
		},
	}
	lsCache := map[string]Model{}
	lspCache := map[string]Model{}
	for i := range lsCacheList {
		lsCache[lsCacheList[i].(*testLogicalSwitch).UUID] = lsCacheList[i]
	}
	for i := range lspCacheList {
		lspCache[lspCacheList[i].(*testLogicalSwitchPort).UUID] = lspCacheList[i]
	}
	cache.cache["Logical_Switch"] = &RowCache{cache: lsCache}
	cache.cache["Logical_Switch_Port"] = &RowCache{cache: lspCache}

	test := []struct {
		name   string
		input  Model
		result *Operation
		err    bool
	}{
		{
			name:  "empty",
			input: &testLogicalSwitch{},
			result: &Operation{
				Op:       "insert",
				Table:    "Logical_Switch",
				Row:      map[string]interface{}{},
				UUIDName: "",
			},
			err: false,
		},
		{
			name: "With some values",
			input: &testLogicalSwitch{
				Name: "foo",
			},
			result: &Operation{
				Op:       "insert",
				Table:    "Logical_Switch",
				Row:      map[string]interface{}{"name": "foo"},
				UUIDName: "",
			},
			err: false,
		},
		{
			name: "With named UUID ",
			input: &testLogicalSwitch{
				UUID: "foo",
			},
			result: &Operation{
				Op:       "insert",
				Table:    "Logical_Switch",
				Row:      map[string]interface{}{},
				UUIDName: "foo",
			},
			err: false,
		},
	}
	for _, tt := range test {
		t.Run(fmt.Sprintf("ApiCreate: %s", tt.name), func(t *testing.T) {
			api := newAPI(cache)
			op, err := api.Create(tt.input)
			if tt.err {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equalf(t, tt.result, op, "Operation should match")
			}
		})
	}
}
