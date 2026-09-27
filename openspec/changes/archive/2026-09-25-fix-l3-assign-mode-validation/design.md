# Design: fix-l3-assign-mode-validation

提前到 assign_partition（图构建期）而非 connect：错误更早更明确；组合本身无效，不引入配置开关。
