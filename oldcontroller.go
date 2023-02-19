package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/anandxkumar/kluster/pkg/apis/ak.dev/v1alpha1"
	clientset "github.com/anandxkumar/kluster/pkg/client/clientset/versioned"
	samplescheme "github.com/anandxkumar/kluster/pkg/client/clientset/versioned/scheme"
	informers "github.com/anandxkumar/kluster/pkg/client/informers/externalversions/ak.dev/v1alpha1"
	listers "github.com/anandxkumar/kluster/pkg/client/listers/ak.dev/v1alpha1"
)

const controllerAgentName = "sample-controller"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Foo is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a Foo fails
	// to sync due to a Deployment of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	MessageResourceExists = "Resource %q already exists and is not managed by Foo"
	// MessageResourceSynced is the message used for an Event fired when a Foo
	// is synced successfully
	MessageResourceSynced = "Foo synced successfully"
)

// Controller is the controller implementation for Foo resources
type Controller struct {
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// sampleclientset is a clientset for our own API group
	sampleclientset clientset.Interface

	foosLister listers.KlusterLister
	foosSynced cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

// NewController returns a new sample controller
func NewController(
	kubeclientset kubernetes.Interface,
	sampleclientset clientset.Interface,
	deploymentInformer appsinformers.DeploymentInformer,
	fooInformer informers.KlusterInformer) *Controller {

	// Create event broadcaster
	// Add sample-controller types to the default Kubernetes Scheme so Events can be
	// logged for sample-controller types.
	utilruntime.Must(samplescheme.AddToScheme(scheme.Scheme))
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &Controller{
		kubeclientset:   kubeclientset,
		sampleclientset: sampleclientset,
		foosLister:      fooInformer.Lister(),
		foosSynced:      fooInformer.Informer().HasSynced,
		workqueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Foos"),
		recorder:        recorder,
	}

	klog.Info("Setting up event handlers")
	// Set up an event handler for when Foo resources change
	fooInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueFoo,
		UpdateFunc: func(old, new interface{}) {
			oldFoo := old.(*v1alpha1.Kluster) // Retrieving old and new Foo objects from Kluster
			newFoo := new.(*v1alpha1.Kluster)
			if newFoo == oldFoo {
				return
			}
			controller.enqueueFoo(new)
		},
		DeleteFunc: controller.dequeueFoo,
	})
	// Set up an event handler for when Deployment resources change. This
	// handler will lookup the owner of the given Deployment, and if it is
	// owned by a Foo resource then the handler will enqueue that Foo resource for
	// processing. This way, we don't need to implement custom logic for
	// handling Deployment resources. More info on this pattern:
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
	// deploymentInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
	// 	AddFunc: controller.handleObject,
	// 	UpdateFunc: func(old, new interface{}) {
	// 		newDepl := new.(*appsv1.Deployment)
	// 		oldDepl := old.(*appsv1.Deployment)
	// 		if newDepl.ResourceVersion == oldDepl.ResourceVersion {
	// 			// Periodic resync will send update events for all known Deployments.
	// 			// Two different versions of the same Deployment will always have different RVs.
	// 			return
	// 		}
	// 		controller.handleObject(new)
	// 	},
	// 	DeleteFunc: controller.handleDel,
	// })

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(workers int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash() // Error handling
	defer c.workqueue.ShutDown()    // Till work queue get empty

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting Foo controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.foosSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Launch k workers to process Foo resources
	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh // waiting for channel to return all queries
	klog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	item, shutdown := c.workqueue.Get()
	if shutdown {
		klog.Info("Shutting down")
		return false
	}

	// we Forget this item so it does not get queued again until another change happens.
	defer c.workqueue.Forget(item)

	// Get the key from the object
	key, err := cache.MetaNamespaceKeyFunc(item)
	if err != nil {
		klog.Errorf("error while calling Namespace Key func on cache for item %s: %s", item, err.Error())
		return false
	}

	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Errorf("error while splitting key into namespace & name: %s", err.Error())
		return false
	}

	tpod, err := c.foosLister.Klusters(ns).Get(name)
	if err != nil {
		klog.Errorf("error %s, Getting the foo resource from lister.", err.Error())
		return false
	}

	// filter out if required pods are already available or not
	labelSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{
			"controller": tpod.Name,
		},
	}
	listOptions := metav1.ListOptions{
		LabelSelector: labels.Set(labelSelector.MatchLabels).String(),
	}

	// Retrieving all pods belonging to a cluster
	pList, _ := c.kubeclientset.CoreV1().Pods(tpod.Namespace).List(context.TODO(), listOptions)

	// Sync handler will be responsible for syncing the clusters pods according to the foo resource
	if err := c.syncHandler(tpod, pList); err != nil {
		klog.Errorf("Error while syncing the current vs desired state for Pods of Foo %v: %v\n", tpod.Name, err.Error())
		return false
	}

	return true
}

// Find total number of pods running of a foo resource of Kluster type Kind
func (c *Controller) totalPodsUp(foo *v1alpha1.Kluster) int {
	// First we need the label selector to match controller
	labelSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{
			"controller": foo.Name,
		},
	}

	// converting labelSelector Match Labels map into string
	listOptions := metav1.ListOptions{
		LabelSelector: labels.Set(labelSelector.MatchLabels).String(),
	}

	// Retrieve the podLists
	podsList, _ := c.kubeclientset.CoreV1().Pods(foo.Namespace).List(context.TODO(), listOptions)

	upPods := 0

	for _, pod := range podsList.Items {
		if pod.ObjectMeta.DeletionTimestamp.IsZero() && pod.Status.Phase == "Running" {
			upPods++
		}
	}
	return upPods
}

// syncHandler monitors the current state & if current != desired,
// tries to meet the desired state.
func (c *Controller) syncHandler(tpod *v1alpha1.Kluster, pList *corev1.PodList) error {
	var podCreate, podDelete bool
	iterate := tpod.Spec.Count
	deleteIterate := 0
	runningPods := c.totalPodsUp(tpod)

	if runningPods < tpod.Spec.Count {
		if runningPods > 0 {
			podDelete = true
			podCreate = true
			iterate = tpod.Spec.Count
			deleteIterate = runningPods
		} else {
			log.Printf("detected mismatch of replica count for CR %v!!!! expected: %v & have: %v\n\n\n", tpod.Name, tpod.Spec.Count, runningPods)
			podCreate = true
			iterate = tpod.Spec.Count - runningPods
		}
	} else if runningPods > tpod.Spec.Count {
		deleteIterate = runningPods - tpod.Spec.Count
		log.Printf("Deleting %v extra pods\n", deleteIterate)
		podDelete = true
	}

	// Delete extra pod
	if podDelete {
		fmt.Println("did we enter here??")
		for i := 0; i < deleteIterate; i++ {
			err := c.kubeclientset.CoreV1().Pods(tpod.Namespace).Delete(context.TODO(), pList.Items[i].Name, metav1.DeleteOptions{})
			if err != nil {
				log.Printf("Pod deletion failed for CR %v\n", tpod.Name)
				return err
			}
			fmt.Println()
		}
	}

	// Creates pod
	if podCreate {
		for i := 0; i < iterate; i++ {
			nPod, err := c.kubeclientset.CoreV1().Pods(tpod.Namespace).Create(context.TODO(), newPod(tpod), metav1.CreateOptions{})
			if err != nil {
				if errors.IsAlreadyExists(err) {
					// retry (might happen when the same named pod is created again)
					iterate++
				} else {
					return err
				}
			}
			if nPod.Name != "" {
				log.Printf("Pod %v created successfully!\n", nPod.Name)
			}
		}
	}

	return nil
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Foo resource
// with the current status of the resource.
// func (c *Controller) syncHandler(key string) error {
// 	// Convert the namespace/name string into a distinct namespace and name
// 	namespace, name, err := cache.SplitMetaNamespaceKey(key)
// 	if err != nil {
// 		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
// 		return nil
// 	}

// 	// Get the Foo resource with this namespace/name
// 	foo, err := c.foosLister.Klusters(namespace).Get(name)
// 	if err != nil {
// 		// The Foo resource may no longer exist, in which case we stop
// 		// processing.
// 		if errors.IsNotFound(err) {
// 			utilruntime.HandleError(fmt.Errorf("foo '%s' in work queue no longer exists", key))
// 			return nil
// 		}

// 		return err
// 	}

// 	message := foo.Spec.Message
// 	if message == "" {
// 		// We choose to absorb the error here as the worker would requeue the
// 		// resource otherwise. Instead, the next time the resource is updated
// 		// the resource will be queued again.
// 		utilruntime.HandleError(fmt.Errorf("%s: deployment name must be specified", key))
// 		return nil
// 	}

// 	// Get the deployment with the name specified in Foo.spec
// 	deployment, err := c.deploymentsLister.Deployments(foo.Namespace).Get(message)
// 	// If the resource doesn't exist, we'll create it
// 	if errors.IsNotFound(err) {
// 		deployment, err = c.kubeclientset.AppsV1().Deployments(foo.Namespace).Create(context.TODO(), newDeployment(foo), metav1.CreateOptions{})
// 	}

// 	// If an error occurs during Get/Create, we'll requeue the item so we can
// 	// attempt processing again later. This could have been caused by a
// 	// temporary network failure, or any other transient reason.
// 	if err != nil {
// 		return err
// 	}

// 	// If the Deployment is not controlled by this Foo resource, we should log
// 	// a warning to the event recorder and return error msg.
// 	if !metav1.IsControlledBy(deployment, foo) {
// 		msg := fmt.Sprintf(MessageResourceExists, deployment.Name)
// 		c.recorder.Event(foo, corev1.EventTypeWarning, ErrResourceExists, msg)
// 		return fmt.Errorf("%s", msg)
// 	}

// 	// If this number of the replicas on the Foo resource is specified, and the
// 	// number does not equal the current desired replicas on the Deployment, we
// 	// should update the Deployment resource.
// 	if foo.Spec.Count != nil && *foo.Spec.Count != *deployment.Spec.Replicas {
// 		klog.V(4).Infof("Foo %s replicas: %d, deployment replicas: %d", name, *foo.Spec.Count, *deployment.Spec.Replicas)
// 		deployment, err = c.kubeclientset.AppsV1().Deployments(foo.Namespace).Update(context.TODO(), newDeployment(foo), metav1.UpdateOptions{})
// 	}

// 	// If an error occurs during Update, we'll requeue the item so we can
// 	// attempt processing again later. This could have been caused by a
// 	// temporary network failure, or any other transient reason.
// 	if err != nil {
// 		return err
// 	}

// 	// Finally, we update the status block of the Foo resource to reflect the
// 	// current state of the world
// 	// err = c.updateFooStatus(foo, deployment)
// 	// if err != nil {
// 	// 	return err
// 	// }

// 	c.recorder.Event(foo, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
// 	return nil
// }

// func (c *Controller) updateFooStatus(foo *samplev1alpha1.Kluster, deployment *appsv1.Deployment) error {
// 	// NEVER modify objects from the store. It's a read-only, local cache.
// 	// You can use DeepCopy() to make a deep copy of original object and modify this copy
// 	// Or create a copy manually for better performance
// 	fooCopy := foo.DeepCopy()
// 	fooCopy.Spec.Count = deployment.Spec.Replicas
// 	// If the CustomResourceSubresources feature gate is not enabled,
// 	// we must use Update instead of UpdateStatus to update the Status block of the Foo resource.
// 	// UpdateStatus will not allow changes to the Spec of the resource,
// 	// which is ideal for ensuring nothing other than resource status has been updated.
// 	_, err := c.sampleclientset.AkV1alpha1().Klusters(foo.Namespace).Update(context.TODO(), fooCopy, metav1.UpdateOptions{})
// 	return err
// }

// enqueueFoo takes a Foo resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Foo.
func (c *Controller) enqueueFoo(obj interface{}) {
	// var key string
	// var err error
	// if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
	// 	utilruntime.HandleError(err)
	// 	return
	// }
	log.Println("enqueueFoo called !!!")
	c.workqueue.Add(obj)
}

func (c *Controller) dequeueFoo(obj interface{}) {
	// var key string
	// var err error
	// if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
	// 	utilruntime.HandleError(err)
	// 	return
	// }
	log.Println("dequeueFoo called !!!")
	c.workqueue.Done(obj)
}

// Creates the new pod with the specified template
func newPod(tpod *v1alpha1.Kluster) *corev1.Pod {
	labels := map[string]string{
		"controller": tpod.Name,
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels:    labels,
			Name:      fmt.Sprintf(tpod.Name + "-" + strconv.Itoa(rand.Intn(10000000))),
			Namespace: tpod.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(tpod, v1alpha1.SchemeGroupVersion.WithKind("TrackPod")),
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "static-nginx",
					Image: "nginx:latest",
					Env: []corev1.EnvVar{
						{
							Name:  "MESSAGE",
							Value: tpod.Spec.Message,
						},
					},
					Command: []string{
						"/bin/sh",
					},
					Args: []string{
						"-c",
						"while true; do echo '$(MESSAGE)'; sleep 10; done",
					},
				},
			},
		},
	}
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the Foo resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that Foo resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
// func (c *Controller) handleObject(obj interface{}) {
// 	var object metav1.Object
// 	var ok bool
// 	if object, ok = obj.(metav1.Object); !ok {
// 		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
// 		if !ok {
// 			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
// 			return
// 		}
// 		object, ok = tombstone.Obj.(metav1.Object)
// 		if !ok {
// 			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
// 			return
// 		}
// 		klog.V(4).Infof("Recovered deleted object '%s' from tombstone", object.GetName())
// 	}
// 	klog.V(4).Infof("Processing object: %s", object.GetName())
// 	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
// 		// If this object is not owned by a Foo, we should not do anything more
// 		// with it.
// 		if ownerRef.Kind != "Foo" {
// 			return
// 		}

// 		foo, err := c.foosLister.Klusters(object.GetNamespace()).Get(ownerRef.Name)
// 		if err != nil {
// 			klog.V(4).Infof("ignoring orphaned object '%s/%s' of foo '%s'", object.GetNamespace(), object.GetName(), ownerRef.Name)
// 			return
// 		}

// 		c.enqueueFoo(foo)
// 		return
// 	}
// }

// newDeployment creates a new Deployment for a Foo resource. It also sets
// the appropriate OwnerReferences on the resource so handleObject can discover
// the Foo resource that 'owns' it.
// func newDeployment(foo *samplev1alpha1.Kluster) *appsv1.Deployment {
// 	fmt.Println(foo.Spec.Count, " ", foo.Spec.Count)
// 	fmt.Println()
// 	var repl int32 = 4
// 	address := &repl
// 	fmt.Println(address, " :address: ", *address)
// 	labels := map[string]string{
// 		"app":        "nginx",
// 		"controller": foo.Name,
// 	}
// 	return &appsv1.Deployment{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "cluster4",
// 			Namespace: foo.Namespace,
// 			OwnerReferences: []metav1.OwnerReference{
// 				*metav1.NewControllerRef(foo, samplev1alpha1.SchemeGroupVersion.WithKind("Kluster")),
// 			},
// 		},
// 		Spec: appsv1.DeploymentSpec{
// 			Replicas: address,
// 			Selector: &metav1.LabelSelector{
// 				MatchLabels: labels,
// 			},
// 			Template: corev1.PodTemplateSpec{
// 				ObjectMeta: metav1.ObjectMeta{
// 					Labels: labels,
// 				},
// 				Spec: corev1.PodSpec{
// 					Containers: []corev1.Container{
// 						{
// 							Name:  "nginx",
// 							Image: "nginx:latest",
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}
// }
