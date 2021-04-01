# Copyright 2021 Dynatrace LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from dynatrace.opentelemetry.metrics.export.serializer import \
    DynatraceMetricsSerializer as dms

cases_metric_keys = [
    ("valid base case", "basecase", "basecase"),
    ("valid base case", "just.a.normal.key", "just.a.normal.key"),
    ("valid leading underscore", "_case", "_case"),
    ("valid underscore", "case_case", "case_case"),
    ("valid number", "case1", "case1"),
    ("invalid leading number", "1case", "case"),
    ("valid leading uppercase", "Case", "Case"),
    ("valid all uppercase", "CASE", "CASE"),
    ("valid intermittent uppercase", "someCase", "someCase"),
    ("valid multiple sections", "prefix.case", "prefix.case"),
    ("valid multiple sections upper", "This.Is.Valid", "This.Is.Valid"),
    ("invalid multiple sections leading number", "0a.b", "a.b"),
    ("valid multiple section leading underscore", "_a.b", "_a.b"),
    ("valid leading number second section", "a.0", "a.0"),
    ("valid leading number second section 2", "a.0.c", "a.0.c"),
    ("valid leading number second section 3", "a.0b.c", "a.0b.c"),
    ("invalid leading hyphen", "-dim", "dim"),
    ("valid trailing hyphen", "dim-", "dim-"),
    ("valid trailing hyphens", "dim---", "dim---"),
    ("invalid empty", "", ""),
    ("invalid only number", "000", ""),
    ("invalid key first section only number", "0.section", ""),
    ("invalid leading character", "~key", "key"),
    ("invalid leading characters", "~0#key", "key"),
    ("invalid intermittent character", "some~key", "some_key"),
    ("invalid intermittent characters", "some#~äkey", "some_key"),
    ("invalid two consecutive dots", "a..b", "a.b"),
    ("invalid five consecutive dots", "a.....b", "a.b"),
    ("invalid just a dot", ".", ""),
    ("invalid leading dot", ".a", ""),
    ("invalid trailing dot", "a.", "a"),
    ("invalid enclosing dots", ".a.", ""),
    ("valid consecutive leading underscores", "___a", "___a"),
    ("valid consecutive trailing underscores", "a___", "a___"),
    ("invalid delete trailing invalid chars", "a$%@", "a"),
    ("invalid delete trailing invalid chars groups", "a.b$%@.c", "a.b.c"),
    ("valid consecutive enclosed underscores", "a___b", "a___b"),
    ("invalid mixture dots underscores", "._._._a_._._.", ""),
    ("valid mixture dots underscores 2", "_._._.a_._", "_._._.a_._"),
    ("invalid empty section", "an..empty.section", "an.empty.section"),
    ("invalid characters", "a,,,b  c=d\\e\\ =,f", "a_b_c_d_e_f"),
    ("invalid characters long",
     "a!b\"c#d$e%f&g'h(i)j*k+l,m-n.o/p:q;r<s=t>u?v@w[x]y\\z^0 1_2;3{4|5}6~7",
     "a_b_c_d_e_f_g_h_i_j_k_l_m-n.o_p_q_r_s_t_u_v_w_x_y_z_0_1_2_3_4_5_6_7"),
    ("invalid trailing characters", "a.b.+", "a.b"),
    ("valid combined test", "metric.key-number-1.001",
     "metric.key-number-1.001"),
    ("valid example 1", "MyMetric", "MyMetric"),
    ("invalid example 1", "0MyMetric", "MyMetric"),
    ("invalid example 2", "mÄtric", "m_tric"),
    ("invalid example 3", "metriÄ", "metri"),
    ("invalid example 4", "Ätric", "tric"),
    ("invalid example 5", "meträääääÖÖÖc", "metr_c"),
    ("invalid truncate key too long", "a" * 270, "a" * 250),
]


@pytest.mark.parametrize("msg,inp,exp", cases_metric_keys,
                         ids=[x[0] for x in cases_metric_keys])
def test_parametrized_normalize_metric_key(msg, inp, exp):
    assert dms._normalize_metric_key(inp) == exp


cases_dimension_keys = [
    ("valid case", "dim", "dim"),
    ("valid number", "dim1", "dim1"),
    ("valid leading underscore", "_dim", "_dim"),
    ("invalid leading uppercase", "Dim", "dim"),
    ("invalid internal uppercase", "dIm", "dim"),
    ("invalid trailing uppercase", "diM", "dim"),
    ("invalid all uppercase", "DIM", "dim"),
    ("valid dimension colon", "dim:dim", "dim:dim"),
    ("valid dimension underscore", "dim_dim", "dim_dim"),
    ("valid dimension hyphen", "dim-dim", "dim-dim"),
    ("invalid leading hyphen", "-dim", "dim"),
    ("valid trailing hyphen", "dim-", "dim-"),
    ("valid trailing hyphens", "dim---", "dim---"),
    ("invalid leading multiple", "~0#dim", "dim"),
    ("invalid leading multiple hyphens", "---dim", "dim"),
    ("invalid leading colon", ":dim", "dim"),
    ("invalid chars", "~@#ä", ""),
    ("invalid trailing chars", "aaa~@#ä", "aaa"),
    ("valid trailing underscores", "aaa___", "aaa___"),
    ("invalid only numbers", "000", ""),
    ("valid compound key", "dim1.value1", "dim1.value1"),
    ("invalid compound leading number", "dim.0dim", "dim.dim"),
    ("invalid compound only number", "dim.000", "dim"),
    ("invalid compound leading invalid char", "dim.~val", "dim.val"),
    ("invalid compound trailing invalid char", "dim.val~~", "dim.val"),
    ("invalid compound only invalid char", "dim.~~~", "dim"),
    ("valid compound leading underscore", "dim._val", "dim._val"),
    ("valid compound only underscore", "dim.___", "dim.___"),
    ("valid compound long", "dim.dim.dim.dim", "dim.dim.dim.dim"),
    ("invalid two dots", "a..b", "a.b"),
    ("invalid five dots", "a.....b", "a.b"),
    ("invalid leading dot", ".a", "a"),
    ("valid colon in compound", "a.b:c.d", "a.b:c.d"),
    ("invalid trailing dot", "a.", "a"),
    ("invalid just a dot", ".", ""),
    ("invalid trailing dots", "a...", "a"),
    ("invalid enclosing dots", ".a.", "a"),
    ("invalid leading whitespace", "   a", "a"),
    ("invalid trailing whitespace", "a   ", "a"),
    ("invalid internal whitespace", "a b", "a_b"),
    ("invalid internal whitespace", "a    b", "a_b"),
    ("invalid empty", "", ""),
    ("valid combined key", "dim.val:count.val001", "dim.val:count.val001"),
    ("invalid characters", "a,,,b  c=d\\e\\ =,f", "a_b_c_d_e_f"),
    ("invalid characters long",
     "a!b\"c#d$e%f&g'h(i)j*k+l,m-n.o/p:q;r<s=t>u?v@w[x]y\\z^0 1_2;3{4|5}6~7",
     "a_b_c_d_e_f_g_h_i_j_k_l_m-n.o_p:q_r_s_t_u_v_w_x_y_z_0_1_2_3_4_5_6_7"),
    ("invalid none", None, ""),
    ("invalid example 1", "Tag", "tag"),
    ("invalid example 2", "0Tag", "tag"),
    ("invalid example 3", "tÄg", "t_g"),
    ("invalid example 4", "mytäääg", "myt_g"),
    ("invalid example 5", "ääätag", "tag"),
    ("invalid example 6", "ä_ätag", "__tag"),
    ("invalid example 7", "Bla___", "bla___"),
    ("invalid truncate key too long", "a" * 120, "a" * 100),
]


@pytest.mark.parametrize("msg,inp,exp", cases_dimension_keys,
                         ids=[x[0] for x in cases_dimension_keys])
def test_parametrized_normalize_dimension_keys(msg, inp, exp):
    assert dms._normalize_dimension_key(inp) == exp


cases_dimension_values = [
    ("valid value", "value", "value"),
    ("valid empty", "", ""),
    ("valid uppercase", "VALUE", "VALUE"),
    ("valid colon", "a:3", "a:3"),
    ("valid value 2", "~@#ä", "~@#ä"),
    ("escape spaces", "a b", "a\\ b"),
    ("escape comma", "a,b", "a\\,b"),
    ("escape equals", "a=b", "a\\=b"),
    ("escape backslash", "a\\b", "a\\\\b"),
    ("escape multiple invalids", " ,=\\", "\\ \\,\\=\\\\"),
    ("escape key-value pair", "key=\"value\"", "key\\=\"value\""),
    # \u0000 NUL character, \u0007 bell character
    ("invalid unicode", "\u0000a\u0007", "a"),
    ("invalid only unicode", "\u0000\u0000", ""),
    ("invalid unicode space", "a\u0001b", "a_b"),
    # 'Ab' in unicode:
    ("valid unicode", "\u0034\u0066", "\u0034\u0066"),
    # A umlaut, a with ring, O umlaut, U umlaut, all valid.
    ("valid unicode", "\u0132_\u0133_\u0150_\u0156",
     "\u0132_\u0133_\u0150_\u0156"),
    ("invalid none", None, ""),
    ("invalid leading unicode NUL", "\u0000a", "a"),
    ("invalid consecutive leading unicode", "\u0000\u0000\u0000a", "a"),
    ("invalid consecutive trailing unicode", "a\u0000\u0000\u0000", "a"),
    ("invalid trailing unicode NUL", "a\u0000", "a"),
    ("invalid enclosed unicode NUL", "a\u0000b", "a_b"),
    (
    "invalid consecutive enclosed unicode NUL", "a\u0000\u0007\u0000b", "a_b"),
    ("invalid truncate value too long", "a" * 270, "a" * 250),
]


@pytest.mark.parametrize("msg,inp,exp", cases_dimension_values,
                         ids=[x[0] for x in cases_dimension_values])
def test_parametrized_normalize_dimension_value(msg, inp, exp):
    assert dms._normalize_dimension_value(inp) == exp
