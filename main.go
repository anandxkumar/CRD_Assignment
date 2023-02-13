package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"time"

	klient "github.com/anandxkumar/kluster/pkg/client/clientset/versioned"
	kInfFac "github.com/anandxkumar/kluster/pkg/client/informers/externalversions"
	"github.com/anandxkumar/kluster/pkg/controller"
	"golang.org/x/net/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func main() {
	var kubeconfig *string

	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Printf("Building config from flags, %s", err.Error())
	}

	klientset, err := klient.NewForConfig(config)
	if err != nil {
		log.Printf(" Getting Client set %s \n", err.Error())
	}

	fmt.Println(klientset)

	clusters, err := klientset.AkV1alpha1().Klusters("").List(context.Background(), metav1.ListOptions{})

	if err != nil {
		log.Printf("Listing clusters Error: %s \n", err.Error())
	}

	log.Printf("Listing clusters is: %d \n", len(clusters.Items))

	infoFactory := kInfFac.NewSharedInformerFactory(klientset, 20*time.Minute)
	ch := make(chan struct{})
	c := controller.NewController(klientset, infoFactory.Ak().V1alpha1().Klusters())

	infoFactory.Start(ch)
	if err := c.Run(ch); err != nil {
		log.Printf("Error running controller %s \n", err.Error())
	}
}
