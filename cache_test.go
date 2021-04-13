package libovsdb

import (
	"testing"

	"encoding/json"
	"github.com/stretchr/testify/assert"
)

type testModel struct {
	UUID string `ovs:"_uuid"`
	Foo  string `ovs:"foo"`
}

func (d *testModel) Table() string {
	return "Open_vSwitch"
}

func TestRowCache_Row(t *testing.T) {
	type fields struct {
		cache map[string]Model
	}
	type args struct {
		uuid string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Model
	}{
		{
			"returns a row that exists",
			fields{cache: map[string]Model{"test": &testModel{}}},
			args{uuid: "test"},
			&testModel{},
		},
		{
			"returns a nil for a row that does not exist",
			fields{cache: map[string]Model{"test": &testModel{}}},
			args{uuid: "foo"},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RowCache{
				cache: tt.fields.cache,
			}
			got := r.Row(tt.args.uuid)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRowCache_Rows(t *testing.T) {
	type fields struct {
		cache map[string]Model
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			"returns a rows that exist",
			fields{cache: map[string]Model{"test1": &testModel{}, "test2": &testModel{}, "test3": &testModel{}}},
			[]string{"test1", "test2", "test3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RowCache{
				cache: tt.fields.cache,
			}
			got := r.Rows()
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestEventHandlerFuncs_OnAdd(t *testing.T) {
	calls := 0
	type fields struct {
		AddFunc    func(table string, row Model)
		UpdateFunc func(table string, old Model, new Model)
		DeleteFunc func(table string, row Model)
	}
	type args struct {
		table string
		row   Model
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"doesn't call nil function",
			fields{nil, nil, nil},
			args{"testTable", &testModel{}},
		},
		{
			"calls onadd function",
			fields{func(string, Model) {
				calls++
			}, nil, nil},
			args{"testTable", &testModel{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventHandlerFuncs{
				AddFunc:    tt.fields.AddFunc,
				UpdateFunc: tt.fields.UpdateFunc,
				DeleteFunc: tt.fields.DeleteFunc,
			}
			e.OnAdd(tt.args.table, tt.args.row)
			if e.AddFunc != nil {
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestEventHandlerFuncs_OnUpdate(t *testing.T) {
	calls := 0
	type fields struct {
		AddFunc    func(table string, row Model)
		UpdateFunc func(table string, old Model, new Model)
		DeleteFunc func(table string, row Model)
	}
	type args struct {
		table string
		old   Model
		new   Model
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"doesn't call nil function",
			fields{nil, nil, nil},
			args{"testTable", &testModel{}, &testModel{}},
		},
		{
			"calls onupdate function",
			fields{nil, func(string, Model, Model) {
				calls++
			}, nil},
			args{"testTable", &testModel{}, &testModel{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventHandlerFuncs{
				AddFunc:    tt.fields.AddFunc,
				UpdateFunc: tt.fields.UpdateFunc,
				DeleteFunc: tt.fields.DeleteFunc,
			}
			e.OnUpdate(tt.args.table, tt.args.old, tt.args.new)
			if e.UpdateFunc != nil {
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestEventHandlerFuncs_OnDelete(t *testing.T) {
	calls := 0
	type fields struct {
		AddFunc    func(table string, row Model)
		UpdateFunc func(table string, old Model, new Model)
		DeleteFunc func(table string, row Model)
	}
	type args struct {
		table string
		row   Model
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"doesn't call nil function",
			fields{nil, nil, nil},
			args{"testTable", &testModel{}},
		},
		{
			"calls ondelete function",
			fields{nil, nil, func(string, Model) {
				calls++
			}},
			args{"testTable", &testModel{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventHandlerFuncs{
				AddFunc:    tt.fields.AddFunc,
				UpdateFunc: tt.fields.UpdateFunc,
				DeleteFunc: tt.fields.DeleteFunc,
			}
			e.OnDelete(tt.args.table, tt.args.row)
			if e.DeleteFunc != nil {
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestTableCache_Table(t *testing.T) {
	type fields struct {
		cache map[string]*RowCache
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *RowCache
	}{
		{
			"returns nil for an empty table",
			fields{
				cache: map[string]*RowCache{"bar": newRowCache()},
			},
			args{
				"foo",
			},
			nil,
		},
		{
			"returns nil for an empty table",
			fields{
				cache: map[string]*RowCache{"bar": newRowCache()},
			},
			args{
				"bar",
			},
			newRowCache(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &TableCache{
				cache: tt.fields.cache,
			}
			got := tr.Table(tt.args.name)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTableCache_Tables(t *testing.T) {
	type fields struct {
		cache map[string]*RowCache
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			"returns a table that exists",
			fields{cache: map[string]*RowCache{"test1": newRowCache(), "test2": newRowCache(), "test3": newRowCache()}},
			[]string{"test1", "test2", "test3"},
		},
		{
			"returns an empty slice if no tables exist",
			fields{cache: map[string]*RowCache{}},
			[]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &TableCache{
				cache: tt.fields.cache,
			}
			got := tr.Tables()
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestTableCache_populate(t *testing.T) {
	t.Log("Create")
	db, err := NewDBModel("Open_vSwitch", []Model{&testModel{}})
	assert.Nil(t, err)
	var schema DatabaseSchema
	err = json.Unmarshal([]byte(`
		 {"name": "TestDB",
		  "tables": {
		    "Open_vSwitch": {
		      "columns": {
		        "foo": {
			  "type": "string"
			}
		      }
		    }
		 }
	     }
	`), &schema)
	assert.Nil(t, err)
	tc := newTableCache(&schema, db)

	testRow := Row{Fields: map[string]interface{}{"_uuid": "test", "foo": "bar"}}
	testRowModel := &testModel{UUID: "test", Foo: "bar"}
	updates := TableUpdates{
		Updates: map[string]TableUpdate{
			"Open_vSwitch": {
				Rows: map[string]RowUpdate{
					"test": {
						Old: Row{},
						New: testRow,
					},
				},
			},
		},
	}
	tc.populate(updates)

	got := tc.cache["Open_vSwitch"].cache["test"]
	assert.Equal(t, testRowModel, got)

	t.Log("Update")
	updatedRow := Row{Fields: map[string]interface{}{"_uuid": "test", "foo": "quux"}}
	updatedRowModel := &testModel{UUID: "test", Foo: "quux"}
	updates = TableUpdates{
		Updates: map[string]TableUpdate{
			"Open_vSwitch": {
				Rows: map[string]RowUpdate{
					"test": {
						Old: testRow,
						New: updatedRow,
					},
				},
			},
		},
	}
	tc.populate(updates)

	got = tc.cache["Open_vSwitch"].cache["test"]
	assert.Equal(t, updatedRowModel, got)

	t.Log("Delete")
	updates = TableUpdates{
		Updates: map[string]TableUpdate{
			"Open_vSwitch": {
				Rows: map[string]RowUpdate{
					"test": {
						Old: updatedRow,
						New: Row{},
					},
				},
			},
		},
	}

	tc.populate(updates)

	_, ok := tc.cache["Open_vSwitch"].cache["test"]
	assert.False(t, ok)
}

func TestEventProcessor_AddEvent(t *testing.T) {
	ep := newEventProcessor(16)
	var events []event
	for i := 0; i < 17; i++ {
		events = append(events, event{
			table:     "bridge",
			eventType: addEvent,
			new: &testModel{
				UUID: "unique",
				Foo:  "bar",
			},
		})
	}
	// overfill channel so event 16 is dropped
	for _, e := range events {
		ep.AddEvent(e.eventType, e.table, nil, e.new)
	}
	// assert channel is full of events
	assert.Equal(t, 16, len(ep.events))

	// read events and ensure they are in FIFO order
	for i := 0; i < 16; i++ {
		event := <-ep.events
		assert.Equal(t, &testModel{UUID: "unique", Foo: "bar"}, event.new)
	}

	// assert channel is empty
	assert.Equal(t, 0, len(ep.events))
}
