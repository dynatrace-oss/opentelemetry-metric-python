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

cases = [
    ("valid base case", "basecase", "basecase"),
    ("valid leading underscore", "_basecase", "_basecase"),
    ("valid underscore", "base_case", "base_case"),
    ("valid number", "basecase1", "basecase1"),
    ("invalid leading number", "1basecase", "basecase"),
    ("valid leading uppercase", "Basecase", "Basecase"),
    ("valid intermittent uppercase", "baseCase", "baseCase"),
    ("valid multiple sections", "prefix.case", "prefix.case"),
    ("valid multiple sections upper", "This.Is.Valid", "This.Is.Valid"),
    ("invalid multiple sections leading number", "0a.b", "a.b"),
    ("valid multiple section leading underscore", "_a.b", "_a.b"),
    ("valid leading number second section", "a.0", "a.0"),
    ("valid leading number second section 2", "a.0.c", "a.0.c"),
    ("valid leading number second section 3", "a.0b.c", "a.0b.c"),
    ("invalid empty", "", ""),
    ("invalid only number", "000", ""),
    ("invalid key first section onl number", "0.section", ""),
    ("invalid leading characters", "~key", "key"),
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
]


@pytest.mark.parametrize("msg,inp,exp", cases, ids = [x[0] for x in cases])
def test_pytest_normalize_metric_key(msg, inp, exp):
    assert dms._normalize_metric_key(inp) == exp
