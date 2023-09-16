package routines

import (
	"cs425-mp2/util"
	"encoding/binary"
	"log"
	"math/rand"
	"net"
	"time"
)

const (
	CONTACT_NUM int = 3		// #alive members to call per period
	MAX_BOOSTRAP_RETY int = 5
)


func StartMembershipListServer(receivePort uint16, introducerAddr string, localList *util.MemberList){

	localAddr, err := net.ResolveUDPAddr("udp4", localList.SelfEntry.Addr())
	if err != nil {
		log.Fatal("Error resolving udp address", err)
	}

	conn, err := net.ListenUDP("udp4", localAddr)

	if introducerAddr != "" {
		boostrapMemberList := getBootstrapMemberList(introducerAddr, localList.Entries.Value.StartUpTs, conn)
		if boostrapMemberList == nil {
			log.Fatal("Member list server failed to boostrap")
		}

		localList.Merge(*boostrapMemberList)
	}


	if err != nil {
		log.Fatal("Failed to start udp server", err)
	}
	
	defer conn.Close()

	go startHeartbeatReciever(receivePort, localList, conn)

	go startHeartbeatSender(localList, conn)

	for {}

}


func startHeartbeatReciever(port uint16, localList *util.MemberList, conn *net.UDPConn){

	buf := make([]byte, util.MAX_ENTRY_NUM*util.ENTRY_SIZE + 5)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err == nil && n > 0 {
			remoteList := util.FromPayload(buf, n)
			if remoteList != nil{
				localList.Merge(*remoteList)
			}
		}
	}
}

func startHeartbeatSender(localList *util.MemberList, conn *net.UDPConn){

	for {
		time.Sleep(time.Duration(util.PERIOD_MILLI)*time.Microsecond)
		localList.IncSelfSeqNum()

		payloads := localList.ToPayloads()
		aliveMembers := localList.AliveMembers()

		if len(aliveMembers) > CONTACT_NUM {
			rand.NewSource(time.Now().UnixNano())
			rand.Shuffle(len(aliveMembers),func(i, j int) {
				aliveMembers[i], aliveMembers[j] = aliveMembers[j], aliveMembers[i] })
			aliveMembers = aliveMembers[:CONTACT_NUM]
		}
		
		for _, ip := range aliveMembers {
			remoteAddr, err := net.ResolveUDPAddr("udp4", ip)
			if err != nil {
				log.Printf("Error resolving remote address %s\n", ip)
				continue
			}
			// todo: add artificial packet drop
			for i, payload := range payloads {
				_, err := conn.WriteToUDP(payload, remoteAddr)
				if (err != nil){
					log.Fatalf("Failed to send  member list %d/%d to %s  %s", i+1, len(payloads), ip, err)
				}
			}
        }
		
	}

}



func getBootstrapMemberList(introducerAddr string, startUpTs int64, conn *net.UDPConn) *util.MemberList {


	addr, err := net.ResolveUDPAddr("udp4", introducerAddr)

	if err != nil {
		log.Fatal("Failed to resolve boostrap server address", err)
	}

	buf := make([]byte, util.MAX_ENTRY_NUM*util.ENTRY_SIZE + 5)
	tsBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBuf, uint64(startUpTs))


	for i :=0 ; i < MAX_BOOSTRAP_RETY; i++ {
		// send join request and advertise startup ts
		conn.WriteToUDP(append([]byte("JOIN"),tsBuf...), addr)
		
		
		n, _, err := conn.ReadFromUDP(buf)
		if (n > 0 && err == nil){
			return util.FromPayload(buf, n)
		}

		log.Printf("Error retrieving bootstrap member list, attempt %d/%d", i+1, MAX_BOOSTRAP_RETY)
	}
	return nil  
}
