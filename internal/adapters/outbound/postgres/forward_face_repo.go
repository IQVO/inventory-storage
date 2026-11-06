package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/domain/allocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ForwardFaceRepo is the pgxpool-backed ports.ForwardFaceStore over
// forward_pick_face_plans / forward_pick_faces (migration 0039). It only
// answers "which bins are this SKU's forward faces": the ORDER in which stock
// is drawn is domain policy (allocation.Order), never SQL, so the adapter and
// the policy cannot disagree.
type ForwardFaceRepo struct {
	pool *pgxpool.Pool
}

func NewForwardFaceRepo(pool *pgxpool.Pool) *ForwardFaceRepo {
	return &ForwardFaceRepo{pool: pool}
}

// ForwardFacesFor returns sku's forward bins across every site's applied map.
func (r *ForwardFaceRepo) ForwardFacesFor(ctx context.Context, sku shared.SKU) (allocation.ForwardFaces, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `SELECT bin_id FROM forward_pick_faces WHERE sku = $1`, sku.String())
	if err != nil {
		return allocation.ForwardFaces{}, err
	}
	defer rows.Close()
	var bins []shared.BinId
	for rows.Next() {
		var bin string
		if err := rows.Scan(&bin); err != nil {
			return allocation.ForwardFaces{}, err
		}
		bins = append(bins, shared.BinId(bin))
	}
	if err := rows.Err(); err != nil {
		return allocation.ForwardFaces{}, err
	}
	return allocation.NewForwardFaces(bins...), nil
}

// ReplaceSiteMap replaces the site's map when m supersedes the held one. The
// supersession guard is the upsert's own WHERE clause (one statement, no
// read-then-write race): a stale or equal plan updates zero rows and the
// assignments are left untouched. It joins the ctx-bound UnitOfWork.
func (r *ForwardFaceRepo) ReplaceSiteMap(ctx context.Context, m allocation.SiteMap) (bool, error) {
	q := querierFrom(ctx, r.pool)
	tag, err := q.Exec(ctx, `
		INSERT INTO forward_pick_face_plans (site_id, plan_id, approved_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (site_id) DO UPDATE SET plan_id = EXCLUDED.plan_id, approved_at = EXCLUDED.approved_at
		WHERE forward_pick_face_plans.approved_at < EXCLUDED.approved_at
		   OR (forward_pick_face_plans.approved_at = EXCLUDED.approved_at AND forward_pick_face_plans.plan_id < EXCLUDED.plan_id)
	`, m.SiteID, m.PlanID, m.ApprovedAt)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if _, err := q.Exec(ctx, `DELETE FROM forward_pick_faces WHERE site_id = $1`, m.SiteID); err != nil {
		return false, err
	}
	skus := make([]string, 0, len(m.Assignments))
	bins := make([]string, 0, len(m.Assignments))
	for _, a := range m.Assignments {
		skus = append(skus, a.SKU.String())
		bins = append(bins, a.Bin.String())
	}
	if len(skus) > 0 {
		if _, err := q.Exec(ctx, `
			INSERT INTO forward_pick_faces (site_id, sku, bin_id)
			SELECT $1, sku, bin_id FROM unnest($2::text[], $3::text[]) AS t(sku, bin_id)
			ON CONFLICT DO NOTHING
		`, m.SiteID, skus, bins); err != nil {
			return false, err
		}
	}
	return true, nil
}
