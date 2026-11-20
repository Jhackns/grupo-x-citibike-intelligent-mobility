# Comparación completa — histórico ampliado

Fuente única: `20261117T171917344468Z`. MAE/RMSE en conteos reportados; R² adicional. Predicciones sin redondear. Modelos y cortes fijados antes de prueba.

## Validación: decide el ganador por combinación

| tipo | horizonte_min | candidato | n_train | n | MAE | RMSE | R2 | duracion_ajuste_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| convencionales | 5 | persistencia | 2824097 | 700836 | 0.07232362 | 0.32078247 | 0.99877513 | 0.00000000 |
| convencionales | 5 | lr_ridge_001 | 2824097 | 700836 | 0.08597957 | 0.32809707 | 0.99871863 | 14.40363576 |
| convencionales | 5 | lr_ridge_01 | 2824097 | 700836 | 0.09346398 | 0.33062599 | 0.99869880 | 12.43469309 |
| convencionales | 5 | lr_sin_regularizar | 2824097 | 700836 | 0.08357812 | 0.32273052 | 0.99876021 | 16.68535843 |
| convencionales | 5 | rf_8_depth4 | 2824097 | 700836 | 1.86003418 | 2.78317161 | 0.90779606 | 49.09655901 |
| convencionales | 5 | rf_16_depth6 | 2824097 | 700836 | 0.67730368 | 1.95365461 | 0.95456766 | 68.70650158 |
| convencionales | 10 | persistencia | 2821248 | 698340 | 0.12950568 | 0.45484759 | 0.99753727 | 0.00000000 |
| convencionales | 10 | lr_ridge_001 | 2821248 | 698340 | 0.14959014 | 0.46029799 | 0.99747789 | 14.48050756 |
| convencionales | 10 | lr_ridge_01 | 2821248 | 698340 | 0.15629531 | 0.46213708 | 0.99745770 | 14.65217020 |
| convencionales | 10 | lr_sin_regularizar | 2821248 | 698340 | 0.14756674 | 0.45626833 | 0.99752186 | 16.87282767 |
| convencionales | 10 | rf_8_depth4 | 2821248 | 698340 | 1.75112189 | 2.69145523 | 0.91376973 | 50.29794872 |
| convencionales | 10 | rf_16_depth6 | 2821248 | 698340 | 0.70023149 | 1.98374625 | 0.95315560 | 68.43121710 |
| convencionales | 15 | persistencia | 2818438 | 695847 | 0.17823315 | 0.55864001 | 0.99628493 | 0.00000000 |
| convencionales | 15 | lr_ridge_001 | 2818438 | 695847 | 0.20415666 | 0.56329372 | 0.99622278 | 15.04370261 |
| convencionales | 15 | lr_ridge_01 | 2818438 | 695847 | 0.21040726 | 0.56479158 | 0.99620267 | 15.44394704 |
| convencionales | 15 | lr_sin_regularizar | 2818438 | 695847 | 0.20317521 | 0.56009853 | 0.99626551 | 17.68477642 |
| convencionales | 15 | rf_8_depth4 | 2818438 | 695847 | 1.78701906 | 2.71471722 | 0.91226929 | 53.70115935 |
| convencionales | 15 | rf_16_depth6 | 2818438 | 695847 | 0.72909113 | 2.02485719 | 0.95119197 | 69.42121443 |
| electricas | 5 | persistencia | 2824097 | 700836 | 0.18218100 | 0.55489457 | 0.99646725 | 0.00000000 |
| electricas | 5 | lr_ridge_001 | 2824097 | 700836 | 0.20514537 | 0.56123103 | 0.99638611 | 11.68203136 |
| electricas | 5 | lr_ridge_01 | 2824097 | 700836 | 0.21228035 | 0.56483221 | 0.99633958 | 13.14714146 |
| electricas | 5 | lr_sin_regularizar | 2824097 | 700836 | 0.20197637 | 0.55628701 | 0.99644950 | 14.76813046 |
| electricas | 5 | rf_8_depth4 | 2824097 | 700836 | 1.36496623 | 2.82529240 | 0.90841616 | 48.65380650 |
| electricas | 5 | rf_16_depth6 | 2824097 | 700836 | 0.83545137 | 2.37539705 | 0.93526122 | 61.67734905 |
| electricas | 10 | persistencia | 2821248 | 698340 | 0.30460234 | 0.77843055 | 0.99304813 | 0.00000000 |
| electricas | 10 | lr_ridge_001 | 2821248 | 698340 | 0.33896536 | 0.78475264 | 0.99293475 | 13.54009513 |
| electricas | 10 | lr_ridge_01 | 2821248 | 698340 | 0.34439118 | 0.78723664 | 0.99288995 | 15.24300295 |
| electricas | 10 | lr_sin_regularizar | 2821248 | 698340 | 0.33657648 | 0.78132213 | 0.99299639 | 14.71185986 |
| electricas | 10 | rf_8_depth4 | 2821248 | 698340 | 1.38651957 | 2.86256867 | 0.90599009 | 47.05385677 |
| electricas | 10 | rf_16_depth6 | 2821248 | 698340 | 0.89671129 | 2.41854354 | 0.93289272 | 64.94207525 |
| electricas | 15 | persistencia | 2818438 | 695847 | 0.40273221 | 0.94891027 | 0.98967035 | 0.00000000 |
| electricas | 15 | lr_ridge_001 | 2818438 | 695847 | 0.44751221 | 0.95658424 | 0.98950260 | 12.51519200 |
| electricas | 15 | lr_ridge_01 | 2818438 | 695847 | 0.45209269 | 0.95862163 | 0.98945784 | 14.96117765 |
| electricas | 15 | lr_sin_regularizar | 2818438 | 695847 | 0.44736340 | 0.95382153 | 0.98956315 | 12.11653097 |
| electricas | 15 | rf_8_depth4 | 2818438 | 695847 | 1.34671947 | 2.81981756 | 0.90878271 | 47.89363628 |
| electricas | 15 | rf_16_depth6 | 2818438 | 695847 | 0.99882926 | 2.49005969 | 0.92886970 | 62.94073499 |

## Prueba posterior a la congelación: no selecciona

| tipo | horizonte_min | candidato | seleccionado | n | MAE | RMSE | R2 | predicciones_negativas_raw |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| convencionales | 5 | persistencia | True | 1399056 | 0.07996535 | 0.34623192 | 0.99862410 | 0 |
| convencionales | 5 | lr_ridge_001 | False | 1399056 | 0.09404152 | 0.35965353 | 0.99851536 | 30867 |
| convencionales | 5 | lr_ridge_01 | False | 1399056 | 0.10162284 | 0.36410551 | 0.99847838 | 13561 |
| convencionales | 5 | lr_sin_regularizar | False | 1399056 | 0.09135302 | 0.34942161 | 0.99859863 | 31604 |
| convencionales | 5 | rf_8_depth4 | False | 1399056 | 1.86563754 | 2.78079979 | 0.91124507 | 0 |
| convencionales | 5 | rf_16_depth6 | False | 1399056 | 0.70384808 | 1.96800164 | 0.95554670 | 0 |
| convencionales | 10 | persistencia | True | 1396457 | 0.14172653 | 0.48616459 | 0.99728707 | 0 |
| convencionales | 10 | lr_ridge_001 | False | 1396457 | 0.16176961 | 0.49489796 | 0.99718873 | 32166 |
| convencionales | 10 | lr_ridge_01 | False | 1396457 | 0.16856021 | 0.49814152 | 0.99715176 | 23795 |
| convencionales | 10 | lr_sin_regularizar | False | 1396457 | 0.15948193 | 0.48726807 | 0.99727475 | 34611 |
| convencionales | 10 | rf_8_depth4 | False | 1396457 | 1.75997771 | 2.68324549 | 0.91735967 | 0 |
| convencionales | 10 | rf_16_depth6 | False | 1396457 | 0.72648364 | 1.99658651 | 0.95424406 | 0 |
| convencionales | 15 | persistencia | True | 1393862 | 0.19407588 | 0.59803741 | 0.99589468 | 0 |
| convencionales | 15 | lr_ridge_001 | False | 1393862 | 0.21952430 | 0.60418611 | 0.99580982 | 32779 |
| convencionales | 15 | lr_ridge_01 | False | 1393862 | 0.22586714 | 0.60682230 | 0.99577318 | 27402 |
| convencionales | 15 | lr_sin_regularizar | False | 1393862 | 0.21822935 | 0.59790984 | 0.99589643 | 35220 |
| convencionales | 15 | rf_8_depth4 | False | 1393862 | 1.79551284 | 2.70918376 | 0.91575043 | 0 |
| convencionales | 15 | rf_16_depth6 | False | 1393862 | 0.75890904 | 2.04016207 | 0.95222287 | 0 |
| electricas | 5 | persistencia | True | 1399056 | 0.18320925 | 0.56764940 | 0.99691279 | 0 |
| electricas | 5 | lr_ridge_001 | False | 1399056 | 0.20658291 | 0.57914479 | 0.99678648 | 58094 |
| electricas | 5 | lr_ridge_01 | False | 1399056 | 0.21396937 | 0.58594800 | 0.99671054 | 38091 |
| electricas | 5 | lr_sin_regularizar | False | 1399056 | 0.20314794 | 0.56844664 | 0.99690411 | 55798 |
| electricas | 5 | rf_8_depth4 | False | 1399056 | 1.46765505 | 3.31316146 | 0.89483007 | 0 |
| electricas | 5 | rf_16_depth6 | False | 1399056 | 0.90220375 | 2.78618353 | 0.92562520 | 0 |
| electricas | 10 | persistencia | True | 1396457 | 0.30528616 | 0.80221349 | 0.99383345 | 0 |
| electricas | 10 | lr_ridge_001 | False | 1396457 | 0.33995423 | 0.80945327 | 0.99372165 | 50813 |
| electricas | 10 | lr_ridge_01 | False | 1396457 | 0.34566287 | 0.81408633 | 0.99364957 | 42913 |
| electricas | 10 | lr_sin_regularizar | False | 1396457 | 0.33734884 | 0.80182847 | 0.99383937 | 52481 |
| electricas | 10 | rf_8_depth4 | False | 1396457 | 1.49359832 | 3.34911030 | 0.89252166 | 0 |
| electricas | 10 | rf_16_depth6 | False | 1396457 | 0.96688343 | 2.83661027 | 0.92289875 | 0 |
| electricas | 15 | persistencia | True | 1393862 | 0.40343664 | 0.98678549 | 0.99066837 | 0 |
| electricas | 15 | lr_ridge_001 | False | 1393862 | 0.44819620 | 0.99141544 | 0.99058059 | 48891 |
| electricas | 15 | lr_ridge_01 | False | 1393862 | 0.45303164 | 0.99498916 | 0.99051256 | 44629 |
| electricas | 15 | lr_sin_regularizar | False | 1393862 | 0.44765294 | 0.98557126 | 0.99069132 | 55449 |
| electricas | 15 | rf_8_depth4 | False | 1393862 | 1.44453631 | 3.29727354 | 0.89581093 | 0 |
| electricas | 15 | rf_16_depth6 | False | 1393862 | 1.06905818 | 2.89105792 | 0.91990124 | 0 |

Los conteos coinciden entre candidatos dentro de cada combinación. La selección sigue siendo la de validación aunque otra métrica de prueba favorezca a un competidor. No se selecciona horizonte final con esta comparación.

## Cambio del conteo entre los extremos: todos los candidatos

| tipo | horizonte_min | candidato | n_cambia | MAE_cambia | n_igual | MAE_igual |
| --- | --- | --- | --- | --- | --- | --- |
| convencionales | 5 | persistencia | 96266 | 1.16215486 | 1302790 | 0.00000000 |
| convencionales | 5 | lr_ridge_001 | 96266 | 1.15994680 | 1302790 | 0.01527945 |
| convencionales | 5 | lr_ridge_01 | 96266 | 1.15985626 | 1302790 | 0.02342766 |
| convencionales | 5 | lr_sin_regularizar | 96266 | 1.15918650 | 1302790 | 0.01244847 |
| convencionales | 5 | rf_8_depth4 | 96266 | 2.21727863 | 1302790 | 1.83965402 |
| convencionales | 5 | rf_16_depth6 | 96266 | 1.69734555 | 1302790 | 0.63043639 |
| convencionales | 10 | persistencia | 158224 | 1.25085322 | 1238233 | 0.00000000 |
| convencionales | 10 | lr_ridge_001 | 158224 | 1.24519999 | 1238233 | 0.02332661 |
| convencionales | 10 | lr_ridge_01 | 158224 | 1.24497453 | 1238233 | 0.03101374 |
| convencionales | 10 | lr_sin_regularizar | 158224 | 1.24429058 | 1238233 | 0.02086281 |
| convencionales | 10 | rf_8_depth4 | 158224 | 2.17933223 | 1238233 | 1.70639171 |
| convencionales | 10 | rf_16_depth6 | 158224 | 1.71813407 | 1238233 | 0.59976848 |
| convencionales | 15 | persistencia | 204211 | 1.32468378 | 1189651 | 0.00000000 |
| convencionales | 15 | lr_ridge_001 | 204211 | 1.31591346 | 1189651 | 0.03132227 |
| convencionales | 15 | lr_ridge_01 | 204211 | 1.31559460 | 1189651 | 0.03880864 |
| convencionales | 15 | lr_sin_regularizar | 204211 | 1.31502524 | 1189651 | 0.02995751 |
| convencionales | 15 | rf_8_depth4 | 204211 | 2.21275904 | 1189651 | 1.72388993 |
| convencionales | 15 | rf_16_depth6 | 204211 | 1.76209815 | 1189651 | 0.58670539 |
| electricas | 5 | persistencia | 200421 | 1.27890790 | 1198635 | 0.00000000 |
| electricas | 5 | lr_ridge_001 | 200421 | 1.27564439 | 1198635 | 0.02782760 |
| electricas | 5 | lr_ridge_01 | 200421 | 1.27531007 | 1198635 | 0.03650504 |
| electricas | 5 | lr_sin_regularizar | 200421 | 1.27454457 | 1198635 | 0.02400218 |
| electricas | 5 | rf_8_depth4 | 200421 | 2.38974033 | 1198635 | 1.31347529 |
| electricas | 5 | rf_16_depth6 | 200421 | 2.13031102 | 1198635 | 0.69685476 |
| electricas | 10 | persistencia | 301748 | 1.41283124 | 1094709 | 0.00000000 |
| electricas | 10 | lr_ridge_001 | 301748 | 1.40555793 | 1094709 | 0.04622888 |
| electricas | 10 | lr_ridge_01 | 301748 | 1.40506230 | 1094709 | 0.05364767 |
| electricas | 10 | lr_sin_regularizar | 301748 | 1.40433760 | 1094709 | 0.04324171 |
| electricas | 10 | rf_8_depth4 | 301748 | 2.33964041 | 1094709 | 1.26039342 |
| electricas | 10 | rf_16_depth6 | 301748 | 2.09726498 | 1094709 | 0.65530257 |
| electricas | 15 | persistencia | 370905 | 1.51611599 | 1022957 | 0.00000000 |
| electricas | 15 | lr_ridge_001 | 370905 | 1.50444569 | 1022957 | 0.06521997 |
| electricas | 15 | lr_ridge_01 | 370905 | 1.50383824 | 1022957 | 0.07202890 |
| electricas | 15 | lr_sin_regularizar | 370905 | 1.50319441 | 1022957 | 0.06493343 |
| electricas | 15 | rf_8_depth4 | 370905 | 2.29770794 | 1022957 | 1.13519230 |
| electricas | 15 | rf_16_depth6 | 370905 | 2.11780780 | 1022957 | 0.68880126 |

Estos estratos son diagnósticos posteriores a la selección y no eligen modelo. Igualdad entre los extremos no acredita ausencia de viajes o movimientos intermedios. Las otras métricas y los estratos de discrepancias completos están en `modelos/subgrupos_prueba.csv`.
