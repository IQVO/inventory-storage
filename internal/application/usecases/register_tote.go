package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// RegisterTote registers a reusable tote by the license plate printed on it
// (ADR 0054, PUT /totes/{lpn}). It is idempotent: a tote that exists is
// returned unchanged with created=false, also to the loser of a concurrent
// registration of the same new LPN. An LPN held by a sealed case is
// ErrLicensePlateTaken: one namespace for every container.
type RegisterTote struct {
	Totes      ports.ToteRepo
	Containers ports.ContainerRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
	// UnitOfWork brackets the claim, the insert and the event (ADR 0017).
	UnitOfWork ports.UnitOfWork
}

// Execute registers the tote lpn.
func (uc *RegisterTote) Execute(ctx context.Context, lpn string) (*container.Tote, bool, error) {
	fresh, err := container.RegisterTote(lpn)
	if err != nil {
		return nil, false, err
	}
	var out *container.Tote
	created := false
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		existing, err := uc.Totes.FindByLPN(ctx, lpn)
		if err != nil || existing != nil {
			out = existing
			return err
		}
		if err := uc.refuseSealedCase(ctx, lpn); err != nil {
			return err
		}
		added, err := uc.Totes.Add(ctx, fresh)
		if err != nil {
			return err
		}
		if !added {
			out, err = uc.lostClaim(ctx, lpn)
			return err
		}
		out, created = fresh, true
		return uc.Events.Publish(ctx, shared.NewToteRegistered(uc.Clock.Now(), lpn))
	})
	if err != nil {
		return nil, false, err
	}
	return out, created, nil
}

// lostClaim answers a lost license-plate claim. A concurrent PUT of the same
// new LPN won it: the winner's tote is answered (created=false, 200), so the
// PUT stays idempotent. Otherwise a sealed case took the plate:
// ErrLicensePlateTaken. carry (Task 5/10 review, ADR 0054).
func (uc *RegisterTote) lostClaim(ctx context.Context, lpn string) (*container.Tote, error) {
	existing, err := uc.Totes.FindByLPN(ctx, lpn)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, ErrLicensePlateTaken
	}
	return existing, nil
}

func (uc *RegisterTote) refuseSealedCase(ctx context.Context, lpn string) error {
	c, err := uc.Containers.FindByLPN(ctx, lpn)
	if err != nil {
		return err
	}
	if c != nil {
		return ErrLicensePlateTaken
	}
	return nil
}
