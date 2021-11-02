package module

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	istioapi "slime.io/slime/framework/apis"
	"slime.io/slime/framework/apis/config/v1alpha1"
	"slime.io/slime/framework/bootstrap"
	basecontroller "slime.io/slime/framework/controllers"
	"slime.io/slime/framework/model"
	"slime.io/slime/framework/model/module"
	lazyloadapiv1alpha1 "slime.io/slime/modules/lazyload/api/v1alpha1"
	"slime.io/slime/modules/lazyload/controllers"
	modmodel "slime.io/slime/modules/lazyload/model"
)

var log = modmodel.ModuleLog

type Module struct {
	//config     *v1alpha1.Config
	env             *bootstrap.Environment
	restConfig      rest.Config
	moduleEventChan chan model.ModuleEvent
}

//func (mo *Module) Config() proto.Message {
//	return mo.config
//}
//
//func (mo *Module) SetConfig(cfg *v1alpha1.Config) {
//	mo.config = cfg
//}

func (mo *Module) Env() *bootstrap.Environment {
	return mo.env
}

func (mo *Module) SetEnv(env *bootstrap.Environment) {
	mo.env = env
}

func (mo *Module) RestConfig() rest.Config {
	return mo.restConfig
}

func (mo *Module) SetRestConfig(cfg rest.Config) {
	mo.restConfig = cfg
}

func (mo *Module) Name() string {
	return modmodel.ModuleName
}

func (mo *Module) InitScheme(scheme *runtime.Scheme) error {
	for _, f := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		lazyloadapiv1alpha1.AddToScheme,
		istioapi.AddToScheme,
	} {
		if err := f(scheme); err != nil {
			return err
		}
	}
	return nil
}

func (mo *Module) InitManager(mgr manager.Manager, env bootstrap.Environment, cbs module.InitCallbacks) error {
	mo.SetEnv(&env)
	mo.SetRestConfig(*mgr.GetConfig())
	mo.moduleEventChan = make(chan model.ModuleEvent)

	sfReconciler := controllers.NewReconciler(mo, mgr)

	var builder basecontroller.ObjectReconcilerBuilder
	if err := builder.Add(basecontroller.ObjectReconcileItem{
		Name: "ServiceFence",
		R:    sfReconciler,
	}).Add(basecontroller.ObjectReconcileItem{
		Name: "VirtualService",
		R: &basecontroller.VirtualServiceReconciler{
			Env:    &env,
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
		},
	}).Add(basecontroller.ObjectReconcileItem{
		Name:    "Service",
		ApiType: &corev1.Service{},
		R:       reconcile.Func(sfReconciler.ReconcileService),
	}).Add(basecontroller.ObjectReconcileItem{
		Name:    "Namespace",
		ApiType: &corev1.Namespace{},
		R:       reconcile.Func(sfReconciler.ReconcileNamespace),
	}).Build(mgr); err != nil {
		log.Errorf("unable to create controller,%+v", err)
		os.Exit(1)
	}

	return nil
}

func (mo *Module) GVKs() []schema.GroupVersionKind {
	return []schema.GroupVersionKind{
		//{
		//	Group:   "",
		//	Version: "v1",
		//	Kind:    "Service",
		//},
		{
			Group:   "networking.istio.io",
			Version: "v1beta1",
			Kind:    "Sidecar",
		},
		// ServicefenceReconciler handlers svf event, metricWatcher handlers other resources
		//{
		//	Group:   "microservice.slime.io",
		//	Version: "v1alpha1",
		//	Kind:    "ServiceFence",
		//},
	}
}

func (mo *Module) MetricUpdateCheckHandler() func(model.WatcherEvent) map[string]*v1alpha1.Prometheus_Source_Handler {
	return func(we model.WatcherEvent) map[string]*v1alpha1.Prometheus_Source_Handler {
		// no other check logic, k8s.MetricSource preCheck is enough
		return mo.Env().Config.Metric.Prometheus.Handlers
	}
}

func (mo *Module) ModuleEventChan() chan model.ModuleEvent {
	return mo.moduleEventChan
}
