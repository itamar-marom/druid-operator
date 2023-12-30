package druid

import (
	"context"
	"fmt"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func createOrUpdateDruidResource(ctx context.Context, client client.Client, obj client.Object, druid *v1alpha1.Druid,
	objState map[string]bool) error {
	_, err := controllerutil.CreateOrUpdate(ctx, client, obj, func() error {
		t := obj.GetCreationTimestamp()
		if (&t).IsZero() {
			addOwnerRefToObject(obj, asOwner(druid))
			if err := addHashToObject(obj); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}
	objState[obj.GetName()] = true
	return nil
}

func sdkCreateOrUpdateAsNeeded(
	ctx context.Context,
	sdk client.Client,
	objFn func() (object, error),
	emptyObjFn func() object,
	isEqualFn func(prev, curr object) bool,
	updaterFn func(prev, curr object),
	drd *v1alpha1.Druid,
	names map[string]bool,
	emitEvent EventEmitter) (DruidNodeStatus, error) {

	if obj, err := objFn(); err != nil {
		return "", err
	} else {
		names[obj.GetName()] = true

		addOwnerRefToObject(obj, asOwner(drd))
		addHashToObject(obj)

		prevObj := emptyObjFn()
		if err := sdk.Get(ctx, *namespacedName(obj.GetName(), obj.GetNamespace()), prevObj); err != nil {
			if apierrors.IsNotFound(err) {
				// resource does not exist, create it.
				create, err := writers.Create(ctx, sdk, drd, obj, emitEvent)
				if err != nil {
					return "", err
				} else {
					return create, nil
				}
			} else {
				e := fmt.Errorf("Failed to get [%s:%s] due to [%s].", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName(), err.Error())
				logger.Error(e, e.Error(), "Prev object", stringifyForLogging(prevObj, drd), "name", drd.Name, "namespace", drd.Namespace)
				emitEvent.EmitEventGeneric(drd, string(druidOjectGetFail), "", err)
				return "", e
			}
		} else {
			// resource already exists, updated it if needed
			if obj.GetAnnotations()[druidOpResourceHash] != prevObj.GetAnnotations()[druidOpResourceHash] || !isEqualFn(prevObj, obj) {

				obj.SetResourceVersion(prevObj.GetResourceVersion())
				updaterFn(prevObj, obj)
				update, err := writers.Update(ctx, sdk, drd, obj, emitEvent)
				if err != nil {
					return "", err
				} else {
					return update, err
				}
			} else {
				return "", nil
			}
		}
	}
}
