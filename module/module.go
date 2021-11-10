package module

import (
	"errors"
	"github.com/golang/protobuf/proto"
	cmap "github.com/orcaman/concurrent-map"
	prometheusApi "github.com/prometheus/client_golang/api"
	prometheusV1 "github.com/prometheus/client_golang/api/prometheus/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	istioapi "slime.io/slime/framework/apis"
	"slime.io/slime/framework/apis/config/v1alpha1"
	"slime.io/slime/framework/bootstrap"
	basecontroller "slime.io/slime/framework/controllers"
	"slime.io/slime/framework/model/metric"
	"slime.io/slime/framework/model/module"
	"slime.io/slime/framework/pkg/metric/source"
	"slime.io/slime/framework/pkg/metric/user"
	lazyloadapiv1alpha1 "slime.io/slime/modules/lazyload/api/v1alpha1"
	"slime.io/slime/modules/lazyload/controllers"
	modmodel "slime.io/slime/modules/lazyload/model"
	"time"
)

var log = modmodel.ModuleLog

type Module struct {
	config v1alpha1.Fence
}

func (mo *Module) Name() string {
	return modmodel.ModuleName
}

func (mo *Module) Config() proto.Message {
	return &mo.config
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

func (mo *Module) InitManager(env bootstrap.Environment, mgr manager.Manager, mc metric.Controller, cbs module.InitCallbacks) error {

	// init metric source
	prometheusSource, err := mo.newPrometheusSource(env)
	if err != nil {
		return err
	}

	// init prometheus user
	users := mo.newUsers(prometheusSource, env)

	// add users to controller
	if err = mc.AddUsers(users); err != nil {
		return err
	}

	// removing users when stopping module
	go func() {
		for {
			select {
			case <-env.Stop:
				log.Infof("removing metric users from controller...")
				if err = mc.RemoveUsers(users); err != nil {
					log.Errorf("remove metric users from controller failed: %v", err)
				} else {
					log.Infof("remove metric users from controller successfully")
				}
				return
			}
		}
	}()

	sfReconciler := controllers.NewReconciler(users, mgr, &env)

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

func (mo *Module) newUsers(source *source.PrometheusSource, env bootstrap.Environment) []metric.User {
	pu := &user.PrometheusUser{
		Name:     "lazyload-gvkEvent-user",
		UserKind: metric.UserKind_GVKEvent,
		Source:   source,
		InterestConfig: user.PrometheusUserInterestConfig{
			InterestKinds: []schema.GroupVersionKind{
				{
					Group:   "networking.istio.io",
					Version: "v1beta1",
					Kind:    "Sidecar",
				},
			},
		},
		InterestMeta: cmap.New(),
		// no other check logic, pre-check is enough
		NeedUpdateMetricHandler: func(event metric.TriggerEvent) map[string]*v1alpha1.Prometheus_Source_Handler {
			return env.Config.Metric.Prometheus.Handlers
		},
		UserEventChan: make(chan metric.UserEvent),
	}

	//init timer user
	tu := &user.TimerUser{
		Name:     "lazyload-timer-user",
		UserKind: metric.UserKind_Timer,
		Source:   source,
		InterestConfig: user.TimerUserInterestConfig{
			InterestInterval: 30 * time.Second,
		},
		InterestMeta: cmap.New(),
		// no other check logic, pre-check is enough
		NeedUpdateMetricHandler: func(event metric.TriggerEvent) map[string]*v1alpha1.Prometheus_Source_Handler {
			return env.Config.Metric.Prometheus.Handlers
		},
		UserEventChan: make(chan metric.UserEvent),
	}

	return []metric.User{pu, tu}
}

func (mo *Module) newPrometheusSource(env bootstrap.Environment) (*source.PrometheusSource, error) {
	ps := env.Config.Metric.Prometheus
	if ps == nil {
		return nil, errors.New("failure create prometheus client, empty prometheus config")
	}
	promClient, err := prometheusApi.NewClient(prometheusApi.Config{
		Address:      ps.Address,
		RoundTripper: nil,
	})
	if err != nil {
		return nil, err
	}

	return &source.PrometheusSource{
		Name:       "lazyload-prometheus-source",
		SourceKind: metric.SourceKind_Prometheus,
		Api:        prometheusV1.NewAPI(promClient),
	}, nil
}
