import { Request, Router } from 'express';
// @ts-ignore
import Model from '../../models/Model';
// @ts-ignore
import { PagedResponseImpl } from '../helpers/PagedResponse';
import { ViewTypes } from 'nocodb-sdk';
// @ts-ignore
import ProjectMgrv2 from '../../db/sql-mgr/v2/ProjectMgrv2';
// @ts-ignore
import Project from '../../models/Project';
import View from '../../models/View';
import ncMetaAclMw from '../helpers/ncMetaAclMw';
import { Tele } from 'nc-help';
import { metaApiMetrics } from '../helpers/apiMetrics';

export async function customViewCreate(req: Request<any, any>, res) {
  Tele.emit('evt', { evt_type: 'vtable:created', show_as: 'custom' });
  const view = await View.insert({
    ...req.body,
    // todo: sanitize
    fk_model_id: req.params.tableId,
    type: ViewTypes.CUSTOM
  });
  res.json(view);
}

const router = Router({ mergeParams: true });
router.post(
  '/api/v1/db/meta/tables/:tableId/customs',
  metaApiMetrics,
  ncMetaAclMw(customViewCreate, 'customViewCreate')
);
export default router;
