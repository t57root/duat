package dht


import (
    "fmt"
    "time"
	"os"
	"bufio"
	"runtime"
)


type printLogger struct{
    p string
}
func (l *printLogger) SetPrefix(s string) {
    l.p = s
}
func (l *printLogger) Debugf(format string, args ...interface{}) {
    fmt.Printf("[debug] " + l.p + " " + format + "\n", args...)
}
func (l *printLogger) Infof(format string, args ...interface{})  {
    fmt.Printf("[info] " + l.p + " " + format + "\n", args...)
}
func (l *printLogger) Errorf(format string, args ...interface{}) {
    fmt.Printf("[error] " + l.p + " " + format + "\n", args...)
}

func NewNew(p int, s []string) *Duat {
    dht1, err := NewDuat(p, s)
    if err != nil {
        fmt.Printf("new duat err: %s\n", err)
    }
    dht1.log.SetPrefix(fmt.Sprintf("p%d", p))
    dht1.Start()
    time.Sleep(time.Second)
    return dht1
}

func Mytest() {
    var ds []*Duat

	end := 1010

	oldgonum := runtime.NumGoroutine()

    d := NewNew(1001, nil)
    ds = append(ds, d)
    for i := 1002; i < end; i++ {
        s := fmt.Sprintf("localhost:%d", i - 1)
        d := NewNew(i, []string{s})
        ds = append(ds, d)
    }
    d = NewNew(end, []string{"localhost:1001"})
    d.outable = true
    ds = append(ds, d)

	ds[3].outable = true

    reader := bufio.NewReader(os.Stdin)
	_ = reader
    // fmt.Print("Enter1: ")
    // reader.ReadString('\n')

    ih, e := DecodeInfoHash("deca7a89a1dbdc4b213de1c0d5351e92582f31fb")
    if e != nil {
        fmt.Printf("e: %s", e)
        return
    }
	/*

			for _, d := range ds {
				d.castFindNode(ih)
				time.Sleep(time.Second *2)
			}
	*/

    reader = bufio.NewReader(os.Stdin)
    fmt.Print("Enter2: ")
    reader.ReadString('\n')

    for _, d := range ds {
        d.Stop()
    }

    for _, d := range ds {
        fmt.Printf("rt of %d: %d\n", d.netPort, d.routingTable.length())

        x := d.routingTable.lookup(ih)
        // d.requestAnnouncePeer(x.address, ih, d.netPort, )
        r := ""
        for _, i := range x {
            r += fmt.Sprintf("\t%x", i.id)
        }
        fmt.Printf("%s\n", r)
    }

    for _, d := range ds {
        peerContacts := d.peerStore.peerContacts(ih)
        count := d.peerStore.count(ih)
        fmt.Printf("peers on %d(%d):\n", d.netPort, count)
		for _, v := range peerContacts {
			fmt.Printf("\t%x %d\n", v.addr, v.cost)
		}
    }

	// time.Sleep(time.Second * 30)
	newgonum := runtime.NumGoroutine()
	fmt.Printf("old: %d, now: %d\n", oldgonum, newgonum)
}
