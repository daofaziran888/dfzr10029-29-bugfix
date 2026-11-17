"""数据集市度量明细包 / 核对包的服务层回归测试。

只依赖标准库，直接跑：
    cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest

from app.services import data_mart as mart


class MartTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # 服务把核对包与回执放在模块级状态里，测试之间互相隔离。
        mart.PACKS[:] = [dict(pack) for pack in _PACK_SNAPSHOT]
        mart.RESULTS.clear()
        mart.RECEIPTS.clear()
        self.service = mart.DataMartService()

    # ---------- 去重与度量一致性 ----------
    def test_collapse_deduplicates_by_fact_key(self) -> None:
        detail = self.service.build_detail(table_no="DATA-0003", version="V3")
        self.assertEqual(detail["fact_row_count"], 5)
        self.assertEqual(detail["row_count"], 3)
        self.assertEqual(detail["measure_total"], 2500.0)
        self.assertEqual(detail["fact_measure_total"], 4500.0)
        self.assertEqual(detail["duplicate_measure_total"], 2000.0)
        self.assertTrue(detail["consistent"])
        order_1001 = next(item for item in detail["items"] if item["fact_key"] == "ORDER-1001")
        self.assertEqual(order_1001["measure"], 1200.0)
        self.assertEqual(order_1001["dimension_members"], ["华东", "华南"])
        self.assertEqual(order_1001["source_rows"], 2)
        self.assertEqual(order_1001["duplicate_measure"], 1200.0)

    def test_measure_total_matches_fact_version_baseline(self) -> None:
        for version in ("V2", "V3"):
            detail = self.service.build_detail(table_no="DATA-0003", version=version)
            baseline = detail["baseline"]
            self.assertEqual(detail["row_count"], baseline["fact_key_count"])
            self.assertAlmostEqual(detail["measure_total"], baseline["measure_total"], places=2)

    def test_allocate_keeps_total_and_splits_by_member(self) -> None:
        detail = self.service.build_detail(table_no="DATA-0003", version="V3", mode="allocate")
        self.assertEqual(detail["row_count"], 5)
        self.assertAlmostEqual(detail["measure_total"], 2500.0, places=2)
        shares = [item["measure"] for item in detail["items"] if item["fact_key"] == "ORDER-1001"]
        self.assertEqual(shares, [600.0, 600.0])
        self.assertTrue(all(item["member"] for item in detail["items"]))

    def test_allocate_remainder_is_absorbed_by_last_member(self) -> None:
        # ORDER-2003 的 1000 摊到三个成员除不尽：333.33 / 333.33 / 333.34，合计仍是 1000。
        detail = self.service.build_detail(table_no="DATA-0002", version="V1", mode="allocate")
        shares = [item["measure"] for item in detail["items"] if item["fact_key"] == "ORDER-2003"]
        self.assertEqual(shares, [333.33, 333.33, 333.34])
        self.assertAlmostEqual(sum(shares), 1000.0, places=2)
        self.assertAlmostEqual(
            detail["measure_total"], detail["baseline"]["measure_total"], places=2)
        self.assertTrue(detail["consistent"])

    def test_unknown_mode_is_rejected(self) -> None:
        with self.assertRaises(mart.MartScopeError):
            self.service.build_detail(table_no="DATA-0003", version="V3", mode="explode")

    # ---------- 版本边界 ----------
    def test_versions_have_separate_scope(self) -> None:
        v2 = self.service.build_detail(table_no="DATA-0003", version="V2")
        v3 = self.service.build_detail(table_no="DATA-0003", version="V3")
        self.assertEqual(v2["measure_total"], 2000.0)
        self.assertEqual(v3["measure_total"], 2500.0)
        self.assertNotIn("ORDER-1004", {item["fact_key"] for item in v3["items"]})

    def test_unknown_version_is_rejected(self) -> None:
        with self.assertRaises(mart.MartScopeError):
            self.service.build_detail(table_no="DATA-0003", version="V9")

    def test_mismatched_bridge_version_keeps_same_version_bridge(self) -> None:
        detail = self.service.build_detail(
            table_no="DATA-0003", version="V3", bridge_version="V2")
        self.assertEqual(detail["fact_version"], "V3")
        self.assertEqual(detail["bridge_version"], "V2")
        codes = [conflict["code"] for conflict in detail["conflicts"]]
        self.assertIn("VERSION_SCOPE_MISMATCH", codes)
        # V3 的 ORDER-1003 在 V2 桥接里没有成员，但不能因此丢掉这条事实。
        self.assertIn("ORDER-1003", {item["fact_key"] for item in detail["items"]})

    # ---------- 分页错位 ----------
    def test_pagination_keeps_rows_aligned(self) -> None:
        detail = self.service.build_detail(table_no="DATA-0003", version="V3")
        first = self.service.paginate_detail(detail, page=1, size=2)
        second = self.service.paginate_detail(detail, page=2, size=2)
        self.assertEqual([item["fact_key"] for item in first["items"]], ["ORDER-1001", "ORDER-1002"])
        self.assertEqual([item["fact_key"] for item in second["items"]], ["ORDER-1003"])
        self.assertEqual(first["total"], 3)
        self.assertEqual(second["page"], 2)

    # ---------- 核对包 ----------
    def test_pack_detail_has_no_residue_from_other_packs(self) -> None:
        first = self.service.get_pack_detail(1)
        second = self.service.get_pack_detail(3)
        self.assertEqual({item["fact_key"] for item in first["actual_items"]},
                         {"ORDER-1001", "ORDER-1002", "ORDER-1003"})
        self.assertEqual({item["fact_key"] for item in second["actual_items"]},
                         {"ORDER-2001", "ORDER-2002", "ORDER-2003"})

    def test_reconcile_consistent_pack(self) -> None:
        result, message = self.service.reconcile_pack(1)
        self.assertIsNotNone(result)
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "一致")
        self.assertEqual(result["actual_rows"], 3)
        self.assertEqual(result["actual_measure"], 2500.0)
        self.assertIn("一致", message)

    def test_reconcile_conflicting_pack_reports_scope(self) -> None:
        result, _ = self.service.reconcile_pack(2)
        self.assertIsNotNone(result)
        self.assertFalse(result["ok"])
        codes = {conflict["code"] for conflict in result["conflicts"]}
        self.assertIn("MEASURE_TOTAL_MISMATCH", codes)
        self.assertIn("FACT_MEASURE_MISMATCH", codes)

    def test_reconcile_is_idempotent(self) -> None:
        self.service.reconcile_pack(1)
        self.service.reconcile_pack(1)
        self.assertEqual(list(mart.RESULTS), [1])
        self.assertEqual(len(mart.RESULTS), 1)

    def test_allocate_pack_reconciles_with_allocated_detail(self) -> None:
        result, _ = self.service.reconcile_pack(4)
        self.assertIsNotNone(result)
        self.assertEqual(result["mode"], "allocate")
        self.assertTrue(result["ok"])
        self.assertEqual(result["actual_rows"], 5)

    # ---------- 导入校验 / 回执 ----------
    def test_import_same_pack_is_idempotent(self) -> None:
        payload = {"values": {"pack_no": "PACK-0001", "table_no": "DATA-0003", "version": "V3",
                              "expected_rows": 3, "expected_measure": 2500}}
        result = self.service.import_pack(payload)
        self.assertTrue(result["ok"])
        self.assertTrue(result["duplicate"])
        self.assertEqual(result["entry"]["id"], 1)
        self.assertEqual(result["receipt"]["status"], "上传成功")

    def test_import_conflicting_pack_is_rejected(self) -> None:
        payload = {"values": {"pack_no": "PACK-0001", "table_no": "DATA-0003", "version": "V3",
                              "expected_rows": 4, "expected_measure": 3000}}
        result = self.service.import_pack(payload)
        self.assertFalse(result["ok"])
        self.assertEqual(result["conflicts"][0]["code"], "PACK_CONFLICT")
        self.assertEqual(result["receipt"]["status"], "上传失败")

    def test_import_rejects_duplicate_fact_keys_in_collapse_pack(self) -> None:
        payload = {"values": {"pack_no": "PACK-9001", "table_no": "DATA-0003", "version": "V3",
                              "expected_rows": 2, "expected_measure": 2400,
                              "items": [{"fact_key": "ORDER-1001", "measure": 1200},
                                        {"fact_key": "ORDER-1001", "measure": 1200}]}}
        result = self.service.import_pack(payload)
        self.assertFalse(result["ok"])
        self.assertIn("事实键 ORDER-1001 重复", "；".join(result["receipt"]["reasons"]))

    def test_import_new_pack_then_reimport_keeps_one_row(self) -> None:
        payload = {"values": {"pack_no": "PACK-9002", "table_no": "DATA-0002", "version": "V1",
                              "expected_rows": 2, "expected_measure": 1000,
                              "items": [{"fact_key": "ORDER-2001", "measure": 600},
                                        {"fact_key": "ORDER-2002", "measure": 400}]}}
        first = self.service.import_pack(payload)
        second = self.service.import_pack(payload)
        self.assertTrue(first["ok"])
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual([pack["pack_no"] for pack in mart.PACKS].count("PACK-9002"), 1)

    def test_upload_failure_creates_traceable_receipt(self) -> None:
        result = self.service.upload_pack({"values": {
            "pack_no": "PACK-9003", "table_no": "DATA-0003", "version": "V9", "expected_rows": 3}})
        self.assertFalse(result["ok"])
        self.assertEqual(result["receipt"]["status"], "上传失败")
        self.assertEqual(result["receipt"]["action"], "上传")
        self.assertEqual(self.service.list_receipts()[0]["receipt_no"], result["receipt"]["receipt_no"])

    def test_import_checksum_mismatch_is_rejected(self) -> None:
        payload = {"values": {"pack_no": "PACK-9004", "table_no": "DATA-0003", "version": "V3",
                              "expected_rows": 1, "expected_measure": 1200,
                              "items": [{"fact_key": "ORDER-1001", "measure": 1200}],
                              "checksum": "deadbeef"}}
        result = self.service.import_pack(payload)
        self.assertFalse(result["ok"])
        self.assertIn("校验和不一致", "；".join(result["receipt"]["reasons"]))


_PACK_SNAPSHOT = [dict(pack) for pack in mart.PACKS]


if __name__ == "__main__":
    unittest.main()
