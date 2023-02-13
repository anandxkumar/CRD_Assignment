# Custom Resource Definition Assignment


## Resource Creation 

- For starting minikube `minikube start --driver podman`
- Resource Type = Kluster, Group = ak.dev, version = v1aplha1
- Create resource file `Kluster.yaml` with Spec details
- Added resource definition by `kubectl apply -f crd.yaml`
- Creating an object of CRD by declaring `object.yaml`
- Added crd based object by `kubectl apply -f object.yaml`
- Checking if object is running by `kubeclt get Kluster` or `kubectl get -o wide` for more details:
```
NAME        AGE
kluster-0   51m
```
- `kubectl api-resources | grep Kluster`
```
klusters                          kl           ak.dev/v1alpha1                        true         Kluster
```
- To check API endpoint `kubectl proxy --port=8010`
- In new terminal `curl localhost:8010/apis | grep ak.dev/v1alpha1`
```
% Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  6699    0  6699    0     0   724k      0 --:--:-- --:--:-- --:--:--  817k
          "groupVersion": "ak.dev/v1alpha1",
        "groupVersion": "ak.dev/v1alpha1",
```
- `curl localhost:8010/apis/ak.dev/v1alpha1/namespaces/default/klusters` for checking object

## Controller Creation

- go to cd `home/{user}/go/src/github.com/{github_username}`
- Create folder `kluster/pkg/api/{Group}/{version}`, add `types.go`
- Add definitions of cluster type and its fields in the `types.go` file
- We need to specifiy that the Kluster Spec is a Kubernetes object, for this we will create a `register.go` file in the same directory that will register the Kluster type to scheme.
- `register.go` code can be generated using `https://github.com/kubernetes/code-generator`.
- Create `doc.go` for declaring tabs. `tabs` are basically used to call a particular instruction for all valid instance over the codebase. For eg- `+k8s:deepcopy-gen=package` means that a deep copy must be generated at every package. This is global. If we declare it anywhere else, it will be local. 
- Install `code-generator` by `go get k8s.io/code-generator` (Path: `Home/go/pkg/mod/k8s.io/code-generator@v0.26.1`)
- `execDir=~/go/pkg/mod/k8s.io/code-generator@v0.26.1`
- `"${execDir}"/generate-groups.sh all github.com/anandxkumar/kluster/pkg/client github.com/anandxkumar/kluster/pkg/apis ak.dev:v1alpha1 --go-header-file "${execDir}"/hack/boilerplate.go.txt`
- Now the deepcopy, clientset, Informers and listeners would have been added to the locla directory.
- Create the `main.go`. In `main.go` we will declare the `kubeconfig`, create `clientsets` and the gnerate clusters.
- To add all dependency : `go mod tidy`, then build using `go build` 
- To create CRD, you can create it manually or can use `controller-gen` command i.e. `go install sigs.k8s.io/kustomize/kustomize@latest` and  `controller-gen paths=github.com/anandxkumar/kluster/pkg/apis/ak.dev/v1alpha1  crd:trivialVersions=true rbac:roleName=controller-perms output:crd:artifacts:config=config/crd/bases`
- To run main.go `./kluster`


- To Fix `client/listers/kluster.go `resource error, go create a function in `pkg/apis/ak.dev/v1alpha1/register.go `
```
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}
```

