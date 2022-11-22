package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/gob"
	"flag"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	// Program starts here. This function will spawn the x RC instances.

	// defaults in defaults.go
	// fierst args is coordinator or normal node
	var functionMod string
	if len(os.Args) > 1 {
		functionMod = os.Args[1]
	} else {
		functionMod = default_function
	}
	// second args is the num of nodes
	var instances uint
	if len(os.Args) > 2 {
		instances_int, _ := strconv.Atoi(os.Args[2])
		instances = uint(instances_int)
	} else {
		instances = default_instances
	}

	functionPtr := flag.String("function", functionMod, "coordinator or node")
	vCPUs := flag.Uint("vpcus", default_vCPUs, "amount of VCPUs available")
	instancesPerVCPUPtr := flag.Uint("instances", instances, "Instances per VCPU")
	nPtr := flag.Uint("n", default_n, "Total amount of nodes")
	mPtr := flag.Uint("m", default_m, "Number of committees")
	totalFPtr := flag.Uint("totalF", default_totalF, "Total adversary tolerance in the form of the divisor (1/x)")
	committeeFPtr := flag.Uint("committeeF", default_committeeF, "Committee adversary tolerance in the form of the divisor (1/x)")
	// dPtr := flag.Uint("d", default_d, "d neighbours")
	BPtr := flag.Uint("B", default_B, "block size in bytes")
	nUsersPtr := flag.Uint("nUsers", default_nUsers, "users in system")
	totalCointsPtr := flag.Uint("totalCoins", default_totalCoins, "total coins in system")
	tpsPtr := flag.Uint("tps", default_tps, "transactions per second")
	localPtr := flag.Bool("local", true, "local run on this computer")
	deltaPtr := flag.Uint("delta", default_delta, "delta")
	portsBegin := flag.Uint("ports", default_ip_ports, "default ip port beginning")
	flag.Parse()

	var flagArgs FlagArgs

	flagArgs.function = *functionPtr
	flagArgs.vCPUs = *vCPUs
	flagArgs.instances = *instancesPerVCPUPtr
	flagArgs.n = *nPtr
	flagArgs.m = *mPtr
	flagArgs.totalF = *totalFPtr
	flagArgs.committeeF = *committeeFPtr
	// flagArgs.d = *dPtr
	flagArgs.B = *BPtr
	flagArgs.nUsers = *nUsersPtr
	flagArgs.totalCoins = *totalCointsPtr
	flagArgs.tps = *tpsPtr
	flagArgs.local = *localPtr
	flagArgs.delta = *deltaPtr
	flagArgs.portsBegin = *portsBegin
	// generate a random key to send the P256 curve interface to gob.Register because it wouldnt cooperate
	randomKey := new(PrivKey)
	randomKey.gen()

	// register structs with gob
	gob.Register(IDAGossipMsg{})
	gob.Register([32]uint8{})
	gob.Register(ProposedBlock{})
	gob.Register(KademliaFindNodeMsg{})
	gob.Register(KademliaFindNodeResponse{})
	gob.Register(elliptic.CurveParams{})
	gob.Register(ecdsa.PublicKey{})
	gob.Register(PubKey{})
	gob.Register(randomKey.Pub.Pub.Curve)
	gob.Register(ConsensusMsg{})
	gob.Register(Transaction{})
	gob.Register(FinalBlock{})
	dur := time.Now().Sub(time.Now())
	gob.Register(dur)
	gob.Register(ByteArrayAndTimestamp{})
	gob.Register(RequestBlockAnswer{})

	if flagArgs.local {
		coord = coord_local
		log.Println("local mod")
	} else {
		coord = coord_aws
		log.Println("aws mod")
	}
	log.Println("Coordinator IP: ", coord)

	// ensure some invariants
	if default_kappa > 256 {
		errFatal(nil, "Default kappa was over 256/1byte")
	}

	// runtime.GOMAXPROCS(int(flagArgs.vCPUs))

	if *functionPtr == "coordinator" {
		log.Println("Launching coordinator")
		launchCoordinator(&flagArgs)
	} else {
		launchNodes(&flagArgs)
	}

}

func launchNodes(flagArgs *FlagArgs) {
	log.Println("Launcing ", flagArgs.instances, " instances")
	for i := uint(0); i < flagArgs.instances; i++ {
		go launchNode(flagArgs, i)
	}

	for {

	}
}
