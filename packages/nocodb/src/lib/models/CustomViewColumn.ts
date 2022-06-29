import Noco from '../Noco';
import { CacheGetType, CacheScope, MetaTable } from '../utils/globals';
import View from './View';
import NocoCache from '../cache/NocoCache';

export default class CustomViewColumn {
  id: string;
  title?: string;
  show?: boolean;
  order?: number;

  fk_view_id: string;
  fk_column_id: string;
  project_id?: string;
  base_id?: string;

  constructor(data: CustomViewColumn) {
    Object.assign(this, data);
  }

  public static async get(customViewColumnId: string, ncMeta = Noco.ncMeta) {
    let view =
      customViewColumnId &&
      (await NocoCache.get(
        `${CacheScope.CUSTOM_VIEW_COLUMN}:${customViewColumnId}`,
        CacheGetType.TYPE_OBJECT
      ));
    if (!view) {
      view = await ncMeta.metaGet2(
        null,
        null,
        MetaTable.CUSTOM_VIEW_COLUMNS,
        customViewColumnId
      );
      await NocoCache.set(
        `${CacheScope.CUSTOM_VIEW_COLUMN}:${customViewColumnId}`,
        view
      );
    }
    return view && new CustomViewColumn(view);
  }

  public static async list(
    viewId: string,
    ncMeta = Noco.ncMeta
  ): Promise<CustomViewColumn[]> {
    let views = await NocoCache.getList(CacheScope.CUSTOM_VIEW_COLUMN, [
      viewId
    ]);
    if (!views.length) {
      views = await ncMeta.metaList2(
        null,
        null,
        MetaTable.CUSTOM_VIEW_COLUMNS,
        {
          condition: {
            fk_view_id: viewId
          },
          orderBy: {
            order: 'asc'
          }
        }
      );
      await NocoCache.setList(CacheScope.CUSTOM_VIEW_COLUMN, [viewId], views);
    }
    views.sort(
      (a, b) =>
        (a.order != null ? a.order : Infinity) -
        (b.order != null ? b.order : Infinity)
    );
    return views?.map(v => new CustomViewColumn(v));
  }

  static async insert(column: Partial<CustomViewColumn>, ncMeta = Noco.ncMeta) {
    const insertObj = {
      fk_view_id: column.fk_view_id,
      fk_column_id: column.fk_column_id,
      order: await ncMeta.metaGetNextOrder(MetaTable.CUSTOM_VIEW_COLUMNS, {
        fk_view_id: column.fk_view_id
      }),
      show: column.show,
      project_id: column.project_id,
      base_id: column.base_id
    };
    if (!(column.project_id && column.base_id)) {
      const viewRef = await View.get(column.fk_view_id, ncMeta);
      insertObj.project_id = viewRef.project_id;
      insertObj.base_id = viewRef.base_id;
    }

    const { id, fk_column_id } = await ncMeta.metaInsert2(
      null,
      null,
      MetaTable.CUSTOM_VIEW_COLUMNS,
      insertObj
    );

    await NocoCache.set(`${CacheScope.CUSTOM_VIEW_COLUMN}:${fk_column_id}`, id);

    await NocoCache.appendToList(
      CacheScope.CUSTOM_VIEW_COLUMN,
      [column.fk_view_id],
      `${CacheScope.CUSTOM_VIEW_COLUMN}:${id}`
    );

    return this.get(id, ncMeta);
  }
}
