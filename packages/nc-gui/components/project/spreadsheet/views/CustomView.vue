<template>
  <div>
    <div v-for="row in data" :key="row.row.id">
      <div v-for="col in fields" :key="col.title">
        {{ col.title }}: {{ row.row[col.title] }}
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: "CustomView",
  props: ["meta", "data"],
  computed: {
    fields() {
      const hideCols = ["CreatedAt", "UpdatedAt"];
      return (
        this.meta.columns.filter(
          (c) =>
            !(c.pk && c.ai) &&
            !hideCols.includes(c.title) &&
            !(this.meta.v || []).some((v) => v.bt && v.bt.title === c.title)
        ) || []
      );
    },
  },
};
</script>

<style></style>
