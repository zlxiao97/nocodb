import Noco from '../Noco';
import { CacheGetType, CacheScope, MetaTable } from '../utils/globals';
import { CustomType } from 'nocodb-sdk';
import CustomViewColumn from './CustomViewColumn';
import View from './View';
import NocoCache from '../cache/NocoCache';

export default class CustomView implements CustomType {
  order: number;

  fk_view_id: string;
  columns?: CustomViewColumn[];

  project_id?: string;
  base_id?: string;

  constructor(data: CustomView) {
    Object.assign(this, data);
  }

  public static async get(viewId: string, ncMeta = Noco.ncMeta) {
    let view =
      viewId &&
      (await NocoCache.get(
        `${CacheScope.CUSTOM_VIEW}:${viewId}`,
        CacheGetType.TYPE_OBJECT
      ));
    if (!view) {
      view = await ncMeta.metaGet2(null, null, MetaTable.CUSTOM_VIEW, {
        fk_view_id: viewId
      });
      await NocoCache.set(`${CacheScope.CUSTOM_VIEW}:${viewId}`, view);
    }
    return view && new CustomView(view);
  }

  static async insert(view: Partial<CustomView>, ncMeta = Noco.ncMeta) {
    const insertObj = {
      fk_view_id: view.fk_view_id,
      project_id: view.project_id,
      base_id: view.base_id
    };
    if (!(view.project_id && view.base_id)) {
      const viewRef = await View.get(view.fk_view_id);
      insertObj.project_id = viewRef.project_id;
      insertObj.base_id = viewRef.base_id;
    }
    await ncMeta.metaInsert2(
      null,
      null,
      MetaTable.CUSTOM_VIEW,
      insertObj,
      true
    );

    return this.get(view.fk_view_id, ncMeta);
  }

  static async update(
    formId: string,
    body: Partial<CustomView>,
    ncMeta = Noco.ncMeta
  ) {
    // get existing cache
    const key = `${CacheScope.CUSTOM_VIEW}:${formId}`;
    const o = await NocoCache.get(key, CacheGetType.TYPE_OBJECT);
    if (o) {
      // set cache
      Object.assign(o, body);
      await NocoCache.set(key, o);
    }
    // update meta
    return await ncMeta.metaUpdate(
      null,
      null,
      MetaTable.CUSTOM_VIEW,
      {},
      {
        fk_view_id: formId
      }
    );
  }

  async getColumns(ncMeta = Noco.ncMeta) {
    return (this.columns = await CustomViewColumn.list(
      this.fk_view_id,
      ncMeta
    ));
  }

  static async getWithInfo(formViewId: string, ncMeta = Noco.ncMeta) {
    const form = await this.get(formViewId, ncMeta);
    await form.getColumns(ncMeta);
    return form;
  }
}
