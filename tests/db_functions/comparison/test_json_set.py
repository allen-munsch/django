from django.test import TestCase
from django.db import connection
from django.db.models import F, Value
from django.db.models.functions import JSONSet, JSONRemove, Upper, JSONObject, Upper, Concat, Replace

from django.test.testcases import skipUnlessDBFeature

from ..models import Flying


@skipUnlessDBFeature("has_json_set_function")
class JSONFunctionTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.c1 = Flying.objects.create(
            circus={
                "id": 1,
                "name": "Bingo Monty DuClownPort I",
                "profession": {"active": False, "specialization": ["physical", "bits"]},
            }
        )
        cls.c2 = Flying.objects.create(
            circus={
                "id": 2,
                "name": "Bingo Monty DuClownPort II",
                "profession": {"active": True, "specialization": ["tumbling"]},
            }
        )
        cls.c3 = Flying.objects.create(
            circus={
                "id": 3,
                "name": "Bingo Monty DuClownPort III",
                "profession": {"active": False, "specialization": ["fire tumbling"]},
            }
        )

    def test_json_set_replace_all(self):
        objs = Flying.objects.all()
        name = "Ringo Monty DuClownTown I"
        objs.update(circus=JSONSet("circus", name=Value(name)))
        objs = Flying.objects.all()
        self.assertTrue(all(obj.circus["name"] == name for obj in objs))


    def test_json_set_replace_nested(self):
        objs = Flying.objects.filter(circus__id=1)
        # replace physical bits with physical flips
        objs.update(circus=JSONSet("circus", profession__specialization__1=Value("flips")))
        updated_obj = Flying.objects.filter(circus__id=1).first()
        self.assertEqual(
            "physical flips",
            " ".join(updated_obj.circus["profession"]["specialization"]),
        )

    def test_json_set_insert_array(self):
        objs = Flying.objects.filter(circus__id=2)
        # upsert tumbling to tumbling flips
        objs.update(circus=JSONSet("circus", profession__specialization__1=Value("flips")))
        updated_obj = Flying.objects.filter(circus__id=2).first()
        self.assertEqual(
            "tumbling flips",
            " ".join(updated_obj.circus["profession"]["specialization"]),
        )

    def test_json_set_with_f_expression(self):
        objs = Flying.objects.filter(circus__profession__active=False)
        objs.update(circus=JSONSet("circus", profession__active=Value(True)))
        updated_objs = Flying.objects.all()
        self.assertTrue(all(obj.circus["profession"]["active"] for obj in updated_objs))

    def test_json_set_multiple_operations(self):
        objs = Flying.objects.all()
        objs.update(
            circus=JSONSet(
                "circus",
                name=Upper(F("circus__name")),
                profession__specialization__1=Value("flips"),
            )
        )
        updated_names = set(obj.circus["name"] for obj in objs)
        expected_names = {
            "BINGO MONTY DUCLOWNPORT I",
            "BINGO MONTY DUCLOWNPORT II",
            "BINGO MONTY DUCLOWNPORT III",
        }
        self.assertEqual(updated_names, expected_names)

    # def test_json_remove_single_key(self):
    #     Flying.objects.update(circus=JSONRemove("circus", "$.profession.active"))
    #     for obj in Flying.objects.all():
    #         self.assertNotIn("active", obj.circus["profession"])

    # def test_json_remove_array_element(self):
    #     Flying.objects.filter(circus__id=1).update(
    #         circus=JSONRemove("circus", "$.profession.specialization[1]")
    #     )
    #     obj = Flying.objects.get(circus__id=1)
    #     self.assertEqual(len(obj.circus["profession"]["specialization"]), 1)
    #     self.assertEqual(obj.circus["profession"]["specialization"][0], "physical")

    # def test_json_remove_multiple_paths(self):
    #     Flying.objects.update(
    #         circus=JSONRemove("circus", "$.id", "$.profession.specialization")
    #     )
    #     for obj in Flying.objects.all():
    #         self.assertNotIn("id", obj.circus)
    #         self.assertNotIn("specialization", obj.circus["profession"])

    # def test_json_set_and_remove_combination(self):
    #     Flying.objects.update(
    #         circus=JSONSet(
    #             JSONRemove("circus", "id"),
    #             name=Value("New Circus"),
    #             profession__new_field=Value("Added Field"),
    #         )
    #     )
    #     for obj in Flying.objects.all():
    #         self.assertNotIn("id", obj.circus)
    #         self.assertEqual(obj.circus["name"], "New Circus")
    #         self.assertEqual(obj.circus["profession"]["new_field"], "Added Field")



    # def XXXtest_filter_and_replace_annotate_all(self):
    #     if connection.vendor == "sqlite":
    #         objs = Flying.objects.all()
    #         upper = JSONSet(
    #             field="circus",
    #             fields={
    #                 "$.name": Upper(F("circus__name")),
    #                 "$.profession.specialization[1]": Value("flips"),
    #             },
    #         )
    #         objs.update(circus=upper)
    #         items = Flying.objects.annotate(
    #             screaming_circus=JSONObject(
    #                 name=F("circus__name"),
    #                 s=Upper(
    #                     Replace(
    #                         Concat(
    #                             F("circus__profession__specialization__0"),
    #                             Value(" "),
    #                             F("circus__profession__specialization__1"),
    #                         ),
    #                         Value('"'),
    #                         Value(""),
    #                     )
    #                 ),
    #             )
    #         )
    #         self.assertSetEqual(
    #             {
    #                 " ".join(
    #                     [item.screaming_circus["name"], item.screaming_circus["s"]]
    #                 )
    #                 for item in items
    #             },
    #             {
    #                 "BINGO MONTY DUCLOWNPORT I PHYSICAL FLIPS",
    #                 "BINGO MONTY DUCLOWNPORT II TUMBLING FLIPS",
    #                 "BINGO MONTY DUCLOWNPORT III FIRE TUMBLING FLIPS",
    #             },
    #         )
