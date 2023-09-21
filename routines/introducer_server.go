package routines

import (
	"cs425-mp2/util"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"
)

func StartIntroducer(port string, protocol uint8, localList *util.MemberList) {
	localAddr, err := net.ResolveUDPAddr("udp4", ":"+port)

	if err != nil {
		log.Fatal("Error resolving introducer address", err)
	}

	if protocol != util.G && protocol != util.GS {
		log.Fatal("Failed to start boostrap server: unknown protocol")
	}

	localList.UpdateProtocol(protocol)

	conn, err := net.ListenUDP("udp4", localAddr)
	if err != nil {
		log.Fatal("Failed to start introducer server", err)
	}

	defer conn.Close()
	buf := make([]byte, 20)

	for {

		for i := range buf {
			buf[i] = 0
		}

		// send new joiner local member list
		n, addr, err := conn.ReadFromUDP(buf)
		if err == nil && n > 0 && string(buf[:4]) == "JOIN" {
			startUpTs := int64(binary.LittleEndian.Uint64(buf[4:12]))

			logTime := time.Now().UnixMilli()
			log.Printf("(%d) Entry update: %s - %s-%d\n", logTime, "JOINED", addr.AddrPort().String(), startUpTs)

			newProcessId := fmt.Sprintf("%s-%d", addr.AddrPort().String(), startUpTs)
			util.ProcessLogger.LogJoin(logTime, newProcessId)

			err = localList.AddNewEntry(&util.MemberListEntry{
				Ip:           addr.AddrPort().Addr().As4(),
				Port:         addr.AddrPort().Port(),
				StartUpTs:    startUpTs,
				SeqNum:       0,
				Status:       util.NORMAL,
				ExpirationTs: time.Now().UnixMilli() + util.TIMEOUT_MILLI,
			})

			if err != nil {
				log.Println("Failed to add new joiner to local list", err)
				continue
			}

			payloads := localList.ToPayloads()
			for i, payload := range payloads {
				_, err = conn.WriteToUDP(payload, addr)
				if err != nil {
					log.Printf("Failed to send  member list %d/%d to %s", i+1, len(payloads), string(addr.IP))
				}
			}
		}
	}

}
