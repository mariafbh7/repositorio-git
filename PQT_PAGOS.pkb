   CREATE OR REPLACE PACKAGE BODY ADMSAM_PR.ptr_pkg_sincronizar_pagos
IS
	excepcion_personalizada 				EXCEPTION;
	estado_ejecucion_exitosa	CONSTANT PLS_INTEGER DEFAULT 0;

	PROCEDURE log_ejecucion (inestadolog			  IN		VARCHAR2,
									 indescripcionlog 	  IN		VARCHAR2,
									 infechainicio 		  IN		DATE,
									 infechafinal			  IN		DATE DEFAULT NULL,
									 outestadoejecucion		  OUT PLS_INTEGER,
									 outmensajeejecucion 	  OUT VARCHAR2
									)
	IS
	  PRAGMA AUTONOMOUS_TRANSACTION;
	BEGIN
        DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'log_ejecucion');
		INSERT
		  INTO admsam.ptr_tbl_pkg_rastros_log (dtm_fecha_hora_inicio,
															dtm_fecha_hora_fin,
															str_estado_ejecucion,
															str_descripcion_estado,
															str_estado
														  )
		VALUES (infechainicio, infechafinal, inestadolog, indescripcionlog, 'A'
				 );

		COMMIT;
	EXCEPTION
		WHEN OTHERS
		THEN
			outestadoejecucion := SQLCODE;
			outmensajeejecucion := SUBSTR (SQLERRM, 1, 200);
	END log_ejecucion;

	PROCEDURE existe_archivo (infechainicio			IN 	 DATE,
									  outestadoejecucion 		OUT PLS_INTEGER,
									  outmensajeejecucion		OUT VARCHAR2
									 )
	IS
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'existe_archivo');
		UPDATE admsam.ptr_tbl_archivos arh
			SET arh.str_descripcion_estado = 'NO SE HA CARGADO'
		 WHERE	  arh.str_estado_archivo IN ('ENV')
				 AND arh.str_tipo_archivo IN ('PGO')
				 AND NOT EXISTS
						  (SELECT NULL
							  FROM fcjadm.iftb_file_log@opcbsp flg
							 WHERE flg.txt_file_name = arh.str_nombre_archivo);

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'ETAPA 1, EXISTE_ARCHIVO '
												 || SUBSTR (SQLERRM, 1, 200),
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END existe_archivo;

	PROCEDURE carga_masspay_upload (outestadoejecucion 	OUT PLS_INTEGER,
											  outmensajeejecucion	OUT VARCHAR2
											 )
	IS
	BEGIN
    DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'carga_masspay_upload');
		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_PCTB_MASSPAY_UPLOAD',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		INSERT INTO admsam.ptr_tbl_pctb_masspay_upload (file_name,
																		seq_no,
																		process_status,
																		error_remarks
																	  )
			SELECT mup.file_name,
					 mup.seq_no,
					 mup.process_status,
					 mup.error_remarks
			  FROM admsam.ptr_tbl_archivos arh,
					 fcjadm.pctb_masspay_upload@opcbsp mup
			 WHERE mup.file_name = arh.str_nombre_archivo
             AND arh.str_estado_archivo IN (
                    'ET1',
                    'ET2',
					'ET3'
                )
                AND arh.str_tipo_archivo = 'PGO';

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			outestadoejecucion := SQLCODE;
			outmensajeejecucion := SUBSTR (SQLERRM, 1, 200);
	END carga_masspay_upload;

	PROCEDURE carga_entries (outestadoejecucion	  OUT PLS_INTEGER,
									 outmensajeejecucion   OUT VARCHAR2
									)
	IS
		--fechasproceso	 CLOB DEFAULT '('; IM834986
		--cuentas			 CLOB DEFAULT '('; IM834986
		--separador		 VARCHAR2 (2); IM834986
		--sentenciasql	 VARCHAR2 (32000); IM834986
		TYPE T_NUM_FEC IS RECORD (str_numero_producto_origen admsam.ptr_tbl_pagos_tercero.str_numero_producto_origen%TYPE,
                                  dtm_fecha_proceso_pago     admsam.ptr_tbl_pagos_tercero.dtm_fecha_proceso_pago%TYPE); /*IM834986*/
		TYPE tipo_pagosterceros IS TABLE OF T_NUM_FEC; /*IM834986*/
		pagosterceros tipo_pagosterceros; /*IM834986*/
	BEGIN
    DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'carga_entries');
		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'CARGA ENTRIES',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_ACVW_ALL_AC_ENTRIES',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		--FOR cursorproducto
			--IN (SELECT DISTINCT
		SELECT DISTINCT	str_numero_producto_origen,
						dtm_fecha_proceso_pago
		BULK COLLECT INTO pagosterceros
			FROM (SELECT DISTINCT
                    pgt.str_numero_producto_origen,
                    pgt.dtm_fecha_proceso_pago
                FROM
                    admsam.ptr_tbl_archivos        arc,
                    admsam.ptr_tbl_pagos_tercero   pgt
                WHERE
                    pgt.num_id_archivo = arc.num_id_archivo
                    AND arc.str_estado_archivo = 'ET3'
                    AND pgt.str_nro_ref_fc IS NOT NULL
                    AND pgt.num_valor_iva IS NULL
                    AND pgt.num_valor_comision IS NULL);


			IF NOT pagosterceros.first IS NULL THEN
				FOR i IN pagosterceros.first..pagosterceros.last LOOP /*IM834986*/
					INSERT INTO admsam.ptr_tbl_acvw_all_ac_entries (
						ac_no,
						trn_ref_no,
						trn_dt,
						lcy_amount,
						trn_code,
						drcr_ind,
						amount_tag
					)
						SELECT
							ent.ac_no,
							ent.trn_ref_no,
							trunc(ent.trn_dt),
							ent.lcy_amount,
							ent.trn_code,
							ent.drcr_ind,
							ent.amount_tag
						FROM
							fcjadm.acvw_all_ac_entries@opcbsp ent
						WHERE
							ent.trn_dt = pagosterceros(i).dtm_fecha_proceso_pago /*IM834986*/
							AND ent.ac_no = pagosterceros(i).str_numero_producto_origen; /*IM834986*/

					COMMIT;
				END LOOP;
			END IF;

		/*fechasProceso := fechasProceso || ')';
      cuentas := cuentas || ')'; */

		/* sentenciaSQL := 'INSERT' || CHR(10) ||
                       '  INTO ADMSAM.PTR_TBL_ACVW_ALL_AC_ENTRIES' || CHR(10) ||
                       '       (' || CHR(10) ||
                       '        AC_NO,' || CHR(10) ||
                       '        TRN_REF_NO,' || CHR(10) ||
                       '        TRN_DT,' || CHR(10) ||
                       '        LCY_AMOUNT,' || CHR(10) ||
                       '        TRN_CODE,' || CHR(10) ||
                       '        DRCR_IND,' || CHR(10) ||
                       '        AMOUNT_TAG' || CHR(10) ||
                       '       )' || CHR(10) ||
                       'SELECT ENT.AC_NO,' || CHR(10) ||
                       '       ENT.TRN_REF_NO,' || CHR(10) ||
                       '       TRUNC(ENT.TRN_DT),' || CHR(10) ||
                       '       ENT.LCY_AMOUNT,' || CHR(10) ||
                       '       ENT.TRN_CODE,' || CHR(10) ||
                       '       ENT.DRCR_IND,' || CHR(10) ||
                       '       ENT.AMOUNT_TAG' || CHR(10) ||
                       '  FROM FCJADM.ACVW_ALL_AC_ENTRIES@opcbsp ENT' || CHR(10) ||
                       ' WHERE ENT.TRN_DT IN ' || fechasProceso || CHR(10) ||
                       '   AND ENT.AC_NO IN ' || cuentas;*/


		/*EXECUTE IMMEDIATE
      sentenciaSQL;*/

		log_ejecucion (inestadolog 			 => 'SUCESS',
							indescripcionlog		 => 'CARGA ENTRIES',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END carga_entries;

	PROCEDURE etapa_uno_rechazados (infechainicio			IN 	 DATE,
											  outestadoejecucion 		OUT PLS_INTEGER,
											  outmensajeejecucion		OUT VARCHAR2
											 )
	IS
	BEGIN
    DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_uno_rechazados');
		log_ejecucion (
			inestadolog 			 => 'START',
			indescripcionlog		 => 'ETAPA 1, MARCA ARCHIVO CON ERROR DE CARGA',
			infechainicio			 => infechainicio,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		UPDATE admsam.ptr_tbl_pagos_tercero pgt
			SET pgt.str_estado_pago = 'X'
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_archivos arh,
								fcjadm.iftb_file_log@opcbsp flg
						WHERE 	 arh.str_estado_archivo IN ('ENV')
								AND arh.str_tipo_archivo IN ('PGO')
								AND arh.num_id_archivo = pgt.num_id_archivo
								AND flg.txt_file_name = arh.str_nombre_archivo
								AND flg.flg_file_status IN ('E'));

		UPDATE admsam.ptr_tbl_archivos arh
			SET arh.str_estado_archivo = 'RCH'
		 WHERE EXISTS
					 (SELECT NULL
						 FROM fcjadm.iftb_file_log@opcbsp flg
						WHERE 	 arh.str_estado_archivo IN ('ENV')
								AND arh.str_tipo_archivo IN ('PGO')
								AND flg.txt_file_name = arh.str_nombre_archivo
								AND flg.flg_file_status IN ('E'));

		log_ejecucion (
			inestadolog 			 => 'SUCESS',
			indescripcionlog		 => 'ETAPA 1, MARCA ARCHIVO CON ERROR DE CARGA',
			infechainicio			 => infechainicio,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'ETAPA 1 RECHAZADOS, ERROR:  '
												 || SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_uno_rechazados;

	PROCEDURE etapa_uno_marca_archivo (
		infechainicio			 IN	  DATE,
		outestadoejecucion		 OUT PLS_INTEGER,
		outmensajeejecucion		 OUT VARCHAR2)
	IS
	BEGIN
    DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_uno_marca_archivo');
		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'ETAPA 1, MARCA ARCHIVO',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		UPDATE admsam.ptr_tbl_archivos arh
			SET arh.str_estado_archivo = 'ET1'
		 WHERE	  arh.str_estado_archivo IN ('ENV')
				 AND arh.str_tipo_archivo IN ('PGO')
				 AND EXISTS
						  (SELECT NULL
							  FROM fcjadm.iftb_file_log@opcbsp flg
							 WHERE	  flg.txt_file_name = arh.str_nombre_archivo
									 AND flg.flg_file_status IN ('P'));

		log_ejecucion (inestadolog 			 => 'SUCESS',
							indescripcionlog		 => 'ETAPA 1, MARCA ARCHIVO',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'ETAPA 1 MARCA ARCHIVO, ERROR:  '
												 || SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_uno_marca_archivo;

	PROCEDURE etapa_uno (outestadoejecucion	 OUT PLS_INTEGER,
								outmensajeejecucion	 OUT VARCHAR2
							  )
	IS
		fechainicio   DATE DEFAULT SYSDATE;
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_uno');
		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'INICIO ETAPA 1',
							infechainicio			 => fechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		existe_archivo (infechainicio 		  => fechainicio,
							 outestadoejecucion	  => outestadoejecucion,
							 outmensajeejecucion   => outmensajeejecucion
							);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		etapa_uno_rechazados (infechainicio 		  => fechainicio,
									 outestadoejecucion	  => outestadoejecucion,
									 outmensajeejecucion   => outmensajeejecucion
									);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		etapa_uno_marca_archivo (infechainicio 		  => fechainicio,
										 outestadoejecucion	  => outestadoejecucion,
										 outmensajeejecucion   => outmensajeejecucion
										);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS',
						indescripcionlog		 => 'FINAL ETAPA 1',
						infechainicio			 => fechainicio,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN excepcion_personalizada
		THEN
			NULL;
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'FINAL ETAPA 1, ERROR : '
												 || SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => fechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_uno;

	PROCEDURE etapa_dos_actualiza_seq_no (
		infechainicio			 IN	  DATE,
		outestadoejecucion		 OUT PLS_INTEGER,
		outmensajeejecucion		 OUT VARCHAR2)
	IS
	BEGIN
    DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_dos_actualiza_seq_no');
		log_ejecucion (
						inestadolog 		 => 'START',
							indescripcionlog		 => 'ETAPA 2, ACTUALIZA CONSECUTIVO',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_PAGOS_TERCERO_SEQ_NO',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		INSERT INTO admsam.ptr_tbl_pagos_tercero_seq_no (
			num_id_pago_tercero,
																		 seq_no
																		)
		WITH wt_masspay_upload AS (
			SELECT 
				arh.num_id_archivo,
								 mup.seq_no,
								 mup.process_status,
								 ROW_NUMBER ()
								 OVER (PARTITION BY arh.num_id_archivo
										 ORDER BY arh.num_id_archivo, mup.seq_no
				) AS fila
			FROM
				admsam.ptr_tbl_archivos arh,
				admsam.ptr_tbl_pctb_masspay_upload mup
			WHERE 
				arh.str_estado_archivo IN (
											'ET1',
											'ET2',
											'ET3'
										)
								 AND arh.str_tipo_archivo IN ('PGO')
				AND mup.file_name = arh.str_nombre_archivo
		),
		wt_pago_tercero AS (
			SELECT 
				arh.num_id_archivo,
								 ptr.num_consecutivo_orden_pago,
								 ROW_NUMBER ()
				OVER (PARTITION BY arh.num_id_archivo 
					ORDER BY arh.num_id_archivo, ptr.num_consecutivo_orden_pago
				) AS fila
			FROM 
				admsam.ptr_tbl_archivos arh,
								 admsam.ptr_tbl_pagos_tercero ptr
			WHERE	  
				arh.str_estado_archivo IN (
											'ET1',
											'ET2',
											'ET3'
										)
								 AND arh.str_tipo_archivo IN ('PGO')
								 AND ptr.num_id_archivo = arh.num_id_archivo
				AND ptr.num_consecutivo_orden_pago IS NOT NULL
		)
		SELECT 
			ptr.num_id_pago_tercero, 
			wmu.seq_no
		FROM 
			wt_pago_tercero wpt,
					 wt_masspay_upload wmu,
					 admsam.ptr_tbl_pagos_tercero ptr
		WHERE	  
			ptr.num_id_archivo = wpt.num_id_archivo
			AND ptr.num_consecutivo_orden_pago = wpt.num_consecutivo_orden_pago
					 AND wpt.num_id_archivo = wmu.num_id_archivo
					 AND wpt.fila = wmu.fila;

		UPDATE admsam.ptr_tbl_pagos_tercero ptr
		SET ptr.str_id_fc = (
			SELECT pts.seq_no
						 FROM admsam.ptr_tbl_pagos_tercero_seq_no pts
			WHERE pts.num_id_pago_tercero = ptr.num_id_pago_tercero
		)
		 WHERE EXISTS
		(
			SELECT NULL
						 FROM admsam.ptr_tbl_pagos_tercero_seq_no pts
			WHERE pts.num_id_pago_tercero = ptr.num_id_pago_tercero
		);

		log_ejecucion (
						inestadolog 		 => 'SUCESS',
							indescripcionlog		 => 'ETAPA 2, ACTUALIZA CONSECUTIVO',
							infechainicio			 => infechainicio,
							infechafinal			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog	 =>	 'ETAPA 2 ACTUALIZA CONSECUTIVO, ERROR:  ' || SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion := SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_dos_actualiza_seq_no;

	PROCEDURE etapa_dos_rechazados (
		infechainicio			IN 	 DATE,
											  outestadoejecucion 		OUT PLS_INTEGER,
											  outmensajeejecucion		OUT VARCHAR2
	) IS
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos', action_name => 'etapa_dos_rechazados');
		log_ejecucion (
			inestadolog 		 => 'START',
							indescripcionlog		 => 'ETAPA 2, MARCA RECHAZADOS',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_PAG_TRCR_ERROR_REMARKS',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion
		);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		--09/06/23 GORTIZ: cruzar por el seq_no de flexcube contra el str_id_fc de sam
		INSERT INTO admsam.ptr_tbl_pag_trcr_error_remarks (
			num_id_pago_tercero,
																			error_remarks
																		  )
		WITH wt_masspay_upload AS (
			SELECT 
				arh.num_id_archivo,
								 mup.seq_no,
								 mup.process_status,
				mup.error_remarks
			FROM 
				admsam.ptr_tbl_archivos arh, 
								 admsam.ptr_tbl_pctb_masspay_upload mup
			WHERE 
				arh.str_estado_archivo IN ('ET1')
								 AND arh.str_tipo_archivo IN ('PGO')
				AND mup.file_name = arh.str_nombre_archivo
		),
		wt_pago_tercero AS (
			SELECT 
				arh.num_id_archivo,
								 ptr.num_id_pago_tercero,
				ptr.str_id_fc
			FROM
				admsam.ptr_tbl_archivos arh,
								 admsam.ptr_tbl_pagos_tercero ptr
			WHERE
				arh.str_estado_archivo IN ('ET1')
								 AND arh.str_tipo_archivo IN ('PGO')
								 AND ptr.num_id_archivo = arh.num_id_archivo
				AND ptr.num_id_pago_tercero IS NOT NULL
		)
		SELECT 
			ptr.num_id_pago_tercero, 
			wmu.error_remarks
		FROM 
			wt_pago_tercero wpt,
					 wt_masspay_upload wmu,
					 admsam.ptr_tbl_pagos_tercero ptr
		WHERE
			ptr.num_id_archivo = wpt.num_id_archivo
					 AND ptr.num_id_pago_tercero = wpt.num_id_pago_tercero
					 AND wpt.num_id_archivo = wmu.num_id_archivo
			AND wpt.str_id_fc = wmu.seq_no
					 AND wmu.process_status IN ('E');

		UPDATE admsam.ptr_tbl_pagos_tercero ptr
		SET 
			ptr.str_estado_pago = 'X',
			ptr.str_descripcion_error = (
				SELECT per.error_remarks
						 FROM admsam.ptr_tbl_pag_trcr_error_remarks per
				WHERE per.num_id_pago_tercero = ptr.num_id_pago_tercero
			)
		WHERE EXISTS (
			SELECT NULL
						 FROM admsam.ptr_tbl_pag_trcr_error_remarks per
			WHERE per.num_id_pago_tercero = ptr.num_id_pago_tercero
		);

		log_ejecucion (
			inestadolog 		 => 'SUCESS',
							indescripcionlog		 => 'ETAPA 2, MARCA RECHAZADOS',
							infechainicio			 => infechainicio,
							infechafinal			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog	 =>	 'ETAPA 2 MARCA RECHAZADOS, ERROR:  ' || SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion
			);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion := SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_dos_rechazados;

	PROCEDURE etapa_dos_marca_archivo (
		infechainicio			 IN	  DATE,
		outestadoejecucion		 OUT PLS_INTEGER,
		outmensajeejecucion		 OUT VARCHAR2)
	IS
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_dos_marca_archivo');
		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'ETAPA 2, MARCA ARCHIVO',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		UPDATE admsam.ptr_tbl_archivos arh
			SET arh.str_estado_archivo = 'ET2'
		 WHERE EXISTS
					 (WITH wt_cantidad_pagos_terceros
							 AS (SELECT arh.num_id_archivo,
											arh.str_nombre_archivo,
											(SELECT COUNT (*)
												FROM admsam.ptr_tbl_pctb_masspay_upload mup
											  WHERE mup.file_name =
														  arh.str_nombre_archivo)
												AS cantidad_masspay_upload,
											(SELECT COUNT (*)
												FROM admsam.ptr_tbl_pagos_tercero ptr
											  WHERE		arh.num_id_archivo =
																ptr.num_id_archivo
													  AND ptr.num_consecutivo_orden_pago
																IS NOT NULL)
												AS cantidad_pagos_tercero
									 FROM admsam.ptr_tbl_archivos arh
									WHERE 	 arh.str_estado_archivo IN ('ET1')
											AND arh.str_tipo_archivo IN ('PGO'))
					  SELECT NULL
						 FROM wt_cantidad_pagos_terceros wcp
						WHERE 	 arh.num_id_archivo = wcp.num_id_archivo
								AND wcp.cantidad_masspay_upload =
										 wcp.cantidad_pagos_tercero);

		log_ejecucion (inestadolog 			 => 'SUCESS',
							indescripcionlog		 => 'ETAPA 2, MARCA ARCHIVO',
							infechainicio			 => infechainicio,
							infechafinal			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'ETAPA 2 MARCA ARCHIVO, ERROR:  '
												 || SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_dos_marca_archivo;

	PROCEDURE etapa_dos (
		outestadoejecucion	 OUT PLS_INTEGER,
								outmensajeejecucion	 OUT VARCHAR2
	) IS
		fechainicio   DATE DEFAULT SYSDATE;
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE (module_name => 'ptr_pkg_sincronizar_pagos', action_name => 'etapa_dos');
		log_ejecucion (
			inestadolog 		 => 'START',
							indescripcionlog		 => 'INICIO ETAPA 2',
							infechainicio			 => fechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		carga_masspay_upload (
			outestadoejecucion	  => outestadoejecucion,
									 outmensajeejecucion   => outmensajeejecucion
									);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		etapa_dos_actualiza_seq_no (
			infechainicio 		  => fechainicio,
									 outestadoejecucion	  => outestadoejecucion,
									 outmensajeejecucion   => outmensajeejecucion
									);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		etapa_dos_rechazados (
			infechainicio 		  => fechainicio,
										 outestadoejecucion	  => outestadoejecucion,
										 outmensajeejecucion   => outmensajeejecucion
										);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		etapa_dos_marca_archivo (
			infechainicio 		  => fechainicio,
											 outestadoejecucion	  => outestadoejecucion,
											 outmensajeejecucion   => outmensajeejecucion
											);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		log_ejecucion (
			inestadolog 		 => 'SUCESS',
							indescripcionlog		 => 'FINAL ETAPA 2',
							infechainicio			 => fechainicio,
							infechafinal			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN excepcion_personalizada
		THEN
			NULL;
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog	 =>	 'FINAL ETAPA 2, ERROR '|| SUBSTR (SQLERRM, 1, 200)|| DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => fechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion
			);
			outestadoejecucion := SQLCODE;
			outmensajeejecucion := SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_dos;

	PROCEDURE etapa_tres_rechazados (
		infechainicio			 IN	  DATE,
												outestadoejecucion		 OUT PLS_INTEGER,
												outmensajeejecucion		 OUT VARCHAR2
	) IS
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos', action_name => 'etapa_tres_rechazados');
		log_ejecucion (
			inestadolog 		 => 'START',
							indescripcionlog		 => 'ETAPA 3, MARCA RECHAZADOS',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		UPDATE admsam.ptr_tbl_pagos_tercero ptr
		SET 
			ptr.str_estado_pago = 'X',
			ptr.str_descripcion_error = (
				WITH wt_masspay_upload AS (
					SELECT 
						arh.num_id_archivo,
											mup.seq_no,
											mup.process_status,
						mup.error_remarks
					FROM 
						admsam.ptr_tbl_archivos arh,
											 admsam.ptr_tbl_pctb_masspay_upload mup
					WHERE 	 
						arh.str_estado_archivo IN ('ET2',
												   'ET3')
											AND arh.str_tipo_archivo IN ('PGO')
						AND mup.file_name = arh.str_nombre_archivo
				),
				wt_pago_tercero AS (
					SELECT 
													arh.num_id_archivo,
											ptr.num_id_pago_tercero,
						ptr.str_id_fc
					FROM 
						admsam.ptr_tbl_archivos arh,
											admsam.ptr_tbl_pagos_tercero ptr
					WHERE 	 
						arh.str_estado_archivo IN ('ET2',
												   'ET3')
											AND arh.str_tipo_archivo IN ('PGO')
											AND ptr.num_id_archivo = arh.num_id_archivo
						AND ptr.num_id_pago_tercero IS NOT NULL
				)
					  SELECT wmu.error_remarks
				FROM 
					wt_pago_tercero wpt, 
					wt_masspay_upload wmu
				WHERE 	 
					ptr.num_id_archivo = wpt.num_id_archivo
								AND ptr.num_id_pago_tercero = wpt.num_id_pago_tercero
								AND wpt.num_id_archivo = wmu.num_id_archivo
					AND wpt.str_id_fc = wmu.seq_no
					AND wmu.process_status IN ('E')
			)
		WHERE EXISTS (
			WITH wt_masspay_upload AS (
				SELECT 
					arh.num_id_archivo,
											mup.seq_no,
					mup.process_status											
				FROM 
					admsam.ptr_tbl_archivos arh,
											admsam.ptr_tbl_pctb_masspay_upload mup
				WHERE 	
					arh.str_estado_archivo IN ('ET2',
											   'ET3')
											AND arh.str_tipo_archivo IN ('PGO')
					AND mup.file_name = arh.str_nombre_archivo
			),
			wt_pago_tercero AS (
				SELECT 
													arh.num_id_archivo,
											ptr.num_id_pago_tercero,
					ptr.str_id_fc
				FROM 
					admsam.ptr_tbl_archivos arh,
											admsam.ptr_tbl_pagos_tercero ptr
				WHERE 	 
					arh.str_estado_archivo IN ('ET2',
											   'ET3')
											AND arh.str_tipo_archivo IN ('PGO')
											AND ptr.num_id_archivo = arh.num_id_archivo
					AND ptr.num_id_pago_tercero IS NOT NULL
			)
					  SELECT NULL
			FROM 
				wt_pago_tercero wpt, 
				wt_masspay_upload wmu
			WHERE 	 
				ptr.num_id_archivo = wpt.num_id_archivo
								AND ptr.num_id_pago_tercero = wpt.num_id_pago_tercero
								AND wpt.num_id_archivo = wmu.num_id_archivo
				AND wpt.str_id_fc = wmu.seq_no
				AND wmu.process_status IN ('E')
		);

		log_ejecucion (
			inestadolog 		 => 'SUCESS',
							indescripcionlog		 => 'ETAPA 3, MARCA RECHAZADOS',
							infechainicio			 => infechainicio,
							infechafinal			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog	 =>	 'ETAPA 3 MARCA RECHAZADOS, ERROR:  ' || SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion
			);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion := SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_tres_rechazados;

	PROCEDURE etapa_tres_marca_archivo (
		infechainicio			 IN	  DATE,
		outestadoejecucion		 OUT PLS_INTEGER,
		outmensajeejecucion		 OUT VARCHAR2)
	IS
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_tres_marca_archivo');
		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'ETAPA 3, MARCA ARCHIVO',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		UPDATE admsam.ptr_tbl_archivos arh
			SET arh.str_estado_archivo = 'ET3'
		 WHERE EXISTS
					 (WITH wt_pagos_tercero
							 AS (SELECT ptr.*
									 FROM admsam.ptr_tbl_archivos arh,
											admsam.ptr_tbl_pagos_tercero ptr
									WHERE 	 arh.str_estado_archivo = 'ET2'
											AND arh.num_id_archivo = ptr.num_id_archivo
											AND REGEXP_LIKE (ptr.str_id_fc, '^[0-9]+$')),
							 wt_pagos_tercero_u
							 AS (SELECT arh.num_id_archivo
									 FROM admsam.ptr_tbl_archivos arh
									WHERE 	 arh.str_estado_archivo = 'ET2'
											AND EXISTS
													 (SELECT NULL
														 FROM wt_pagos_tercero ptr,
																admsam.ptr_tbl_pctb_masspay_upload mup
														WHERE 	 arh.str_nombre_archivo =
																		 mup.file_name
																AND ptr.num_id_archivo =
																		 arh.num_id_archivo
																AND mup.seq_no =
																		 ptr.str_id_fc
																AND mup.process_status IN
																		 ('U')))
					  SELECT NULL
						 FROM admsam.ptr_tbl_pctb_masspay_upload mup
						WHERE 	 arh.str_estado_archivo = 'ET2'
								AND mup.file_name = arh.str_nombre_archivo
								AND NOT EXISTS
											  (SELECT NULL
												  FROM wt_pagos_tercero_u wpt
												 WHERE wpt.num_id_archivo =
															 arh.num_id_archivo));

		log_ejecucion (inestadolog 			 => 'SUCESS',
							indescripcionlog		 => 'ETAPA 3, MARCA ARCHIVO',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'ETAPA 2 MARCA ARCHIVO, ERROR:  '
												 || SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_tres_marca_archivo;

	PROCEDURE etapa_tres (outestadoejecucion	  OUT PLS_INTEGER,
								 outmensajeejecucion   OUT VARCHAR2
								)
	IS
		fechainicio   DATE DEFAULT SYSDATE;
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_tres');
		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'INICIO ETAPA 3',
							infechainicio			 => fechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		etapa_tres_rechazados (infechainicio			=> fechainicio,
									  outestadoejecucion 	=> outestadoejecucion,
									  outmensajeejecucion	=> outmensajeejecucion
									 );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		etapa_tres_marca_archivo (infechainicio			=> fechainicio,
										  outestadoejecucion 	=> outestadoejecucion,
										  outmensajeejecucion	=> outmensajeejecucion
										 );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		log_ejecucion (inestadolog 			 => 'SUCESS',
							indescripcionlog		 => 'FINAL ETAPA 3',
							infechainicio			 => fechainicio,
							infechafinal			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN excepcion_personalizada
		THEN
			NULL;
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 => 'FINAL ' || SUBSTR (SQLERRM, 1, 200),
				infechainicio			 => fechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_tres;

	PROCEDURE etapa_cuatro_rechazados (
		infechainicio			 IN	  DATE,
		outestadoejecucion		 OUT PLS_INTEGER,
		outmensajeejecucion		 OUT VARCHAR2)
	IS
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_cuatro_rechazados');

		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'ETAPA 4, MARCA RECHAZADOS',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_PAYMENT_STATUS',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		INSERT INTO admsam.ptr_tbl_payment_status (num_id_pago_tercero,
																 payment_status,
																 err_code,
																 error_reason
																)
			SELECT  /*+INDEX (ptr PTR_IDX_PAGOS_TERCERO_01)*/ ptr.num_id_pago_tercero,
					 mpi.payment_status,
					 mpi.err_code,
					 mpi.error_reason
			  FROM admsam.ptr_tbl_pctm_masspy_pymnt_inpt mpi,
					 admsam.ptr_tbl_pagos_tercero ptr,
					 admsam.ptr_tbl_archivos arc
			 WHERE	  arc.str_estado_archivo IN ('ET3')
					 AND ptr.num_id_archivo = arc.num_id_archivo
					 AND mpi.list_id = ptr.str_numero_control
					 AND mpi.seq_no = ptr.str_id_fc
					 AND mpi.payment_status IN ('X')
					 AND mpi.err_code IS NOT NULL;

		UPDATE admsam.ptr_tbl_pagos_tercero ptr
			SET (ptr.str_estado_pago,
				  ptr.str_codigo_error,
				  ptr.str_descripcion_error,
				  str_tipo_comision,
				  num_valor_iva,
				  num_valor_comision
				 ) =
					 (SELECT mpi.payment_status,
								mpi.err_code,
								mpi.error_reason,
								NULL,
								0,
								0
						 FROM admsam.ptr_tbl_payment_status mpi
						WHERE ptr.num_id_pago_tercero = mpi.num_id_pago_tercero)
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_payment_status mpi
						WHERE ptr.num_id_pago_tercero = mpi.num_id_pago_tercero);

		log_ejecucion (inestadolog 			 => 'END',
							indescripcionlog		 => 'ETAPA 4, MARCA RECHAZADOS',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'ETAPA 4 MARCA RECHAZADOS, ERROR:  '
												 || SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_cuatro_rechazados;

	PROCEDURE etapa_cuatro_marca_estados (
		infechainicio			 IN	  DATE,
		outestadoejecucion		 OUT PLS_INTEGER,
		outmensajeejecucion		 OUT VARCHAR2)
	IS
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_cuatro_marca_estados');

		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'ETAPA 4, MARCA ESTADOS',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_ET4',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;
        -- -----------------------------------------------------------------------
		-- Estado U
		-- -----------------------------------------------------------------------
        -- Registro de inicio para la etapa de marca Estado U
                                        log_ejecucion (
                                            inestadolog => 'START',
                                            indescripcionlog => 'MARCA ESTADO U',
                                            infechainicio => SYSDATE,
                                            outestadoejecucion => outestadoejecucion,
                                            outmensajeejecucion => outmensajeejecucion
                                        );
                                        
                                        -- Inserción de datos en PTR_TBL_ET4 para Estado U
                                        INSERT INTO admsam.ptr_tbl_et4 (
                                            num_id_pago_tercero,
                                            str_ciclo,
                                            str_nro_ref_fc,
                                            descripcion_estado_fc -- Nuevo campo para la descripción del Estado U
                                        )
                                        WITH wt_pagos_tercero AS (
                                           
                                        )
                                        SELECT 
                                            wpt.num_id_pago_tercero,
                                            (CASE
                                                WHEN mpi.cycle NOT IN (1, 2, 3, 4, 5) THEN '1'
                                                ELSE mpi.cycle
                                            END) AS str_ciclo,
                                            mpi.reference_no AS str_nro_ref_fc,
                                           -- mpi.process_status AS descripcion_estado, -- Descripción para el Estado U
                                            mpi.error_reason AS error_reason 
                                        FROM 
                                            admsam.ptr_tbl_pctm_masspy_pymnt_inpt mpi,
                                            wt_pagos_tercero wpt
                                        WHERE 
                                            mpi.list_id = wpt.str_numero_control
                                            AND mpi.seq_no = wpt.str_id_fc
                                            AND mpi.process_status IN ('U'); -- Filtro para Estado U
                                        
                                        COMMIT; -- Confirmación de la transacción
                                        
                                        -- Registro de éxito de la marca Estado U
                                        log_ejecucion (
                                            inestadolog => 'SUCESS',
                                            indescripcionlog => 'MARCA ESTADO U',
                                            infechainicio => SYSDATE,
                                            outestadoejecucion => outestadoejecucion,
                                            outmensajeejecucion => outmensajeejecucion
                                        );

                        -- Registro de inicio de la actualización para Estado U
                        log_ejecucion (
                            inestadolog => 'START UPDATE',
                            indescripcionlog => 'MARCA ESTADO U',
                            infechainicio => SYSDATE,
                            outestadoejecucion => outestadoejecucion,
                            outmensajeejecucion => outmensajeejecucion
                        );
                        
                        -- Actualización en PTR_TBL_PAGOS_TERCERO para Estado U con descripción
                        UPDATE admsam.ptr_tbl_pagos_tercero pgt
                        SET (
                            str_estado_pago,
                            str_ciclo,
                            str_nro_ref_fc,
                            descripcion_estado_fc -- Actualización del campo para descripción del Estado U
                        ) =
                            (SELECT 
                                'U',
                                et4.str_ciclo,
                                et4.str_nro_ref_fc,
                                et4.descripcion_estado_fc -- Campo correspondiente a la descripción Estado U
                            FROM 
                                admsam.ptr_tbl_et4 et4
                        WHERE 
                            et4.num_id_pago_tercero = pgt.num_id_pago_tercero)
                    WHERE 
                        EXISTS (
                            SELECT NULL
                            FROM admsam.ptr_tbl_et4 et4
                            WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero
                        );
                    
                    COMMIT; -- Confirmación de la transacción
                    
                    -- Registro de éxito de la actualización para Estado U
                    log_ejecucion (
                        inestadolog => 'SUCCESS UPDATE',
                        indescripcionlog => 'MARCA ESTADO U',
                        infechainicio => SYSDATE,
                        outestadoejecucion => outestadoejecucion,
                        outmensajeejecucion => outmensajeejecucion
                    );

        
		-- -----------------------------------------------------------------------
		-- Estado D
		-- -----------------------------------------------------------------------
		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'MARCA ESTADO D',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		INSERT INTO admsam.ptr_tbl_et4 (num_id_pago_tercero,
												  str_ciclo,
												  str_nro_ref_fc
												 )
			WITH wt_pagos_tercero
				  AS (SELECT /*+ MATERIALIZE */
								pgt.num_id_pago_tercero,
								 pgt.str_numero_producto_origen,
								 pgt.str_numero_control,
								 pgt.str_id_fc,
								 pgt.str_nro_ref_fc,
								 TRUNC (pgt.dtm_fecha_proceso_pago)
									 AS dtm_fecha_proceso_pago
						  FROM admsam.ptr_tbl_archivos arc,
								 admsam.ptr_tbl_pagos_tercero pgt
						 WHERE	  arc.str_estado_archivo IN ('ET3')
								 AND pgt.str_estado_pago IN ('P')
								 AND pgt.num_id_archivo = arc.num_id_archivo)
			SELECT z.num_id_pago_tercero, z.str_ciclo, z.str_nro_ref_fc
			  FROM (SELECT wpt.num_id_pago_tercero,
								(CASE
									 WHEN mpi.cycle NOT IN (1, 2, 3, 4, 5) THEN '1'
									 ELSE mpi.cycle
								 END)
									AS str_ciclo,
								mpi.reference_no AS str_nro_ref_fc
						 FROM admsam.ptr_tbl_pctm_masspy_pymnt_inpt mpi,
								wt_pagos_tercero wpt
						WHERE 	 mpi.list_id = wpt.str_numero_control
								AND mpi.seq_no = wpt.str_id_fc
								AND mpi.payment_status IN ('D')) z;

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS',
							indescripcionlog		 => 'MARCA ESTADO D',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		log_ejecucion (inestadolog 			 => 'START UPDATE',
							indescripcionlog		 => 'MARCA ESTADO D',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		UPDATE admsam.ptr_tbl_pagos_tercero pgt
			SET (str_estado_pago, str_ciclo, str_nro_ref_fc) =
					 (SELECT 'D', et4.str_ciclo, et4.str_nro_ref_fc
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero)
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero);

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCCESS UPDATE',
							indescripcionlog		 => 'MARCA ESTADO D',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		-- -----------------------------------------------------------------------
		-- Estado R
		-- -----------------------------------------------------------------------

		log_ejecucion (inestadolog 			 => 'START INSERT',
							indescripcionlog		 => 'MARCA ESTADO R',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_ET4',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		INSERT INTO admsam.ptr_tbl_et4 (num_id_pago_tercero,
												  str_ciclo,
												  str_nro_ref_fc,
												  dtm_fecha_cobro
												 )
			WITH wt_pagos_tercero
				  AS (SELECT /*+ MATERIALIZE */
								pgt.num_id_pago_tercero,
								 pgt.str_numero_producto_origen,
								 pgt.str_numero_control,
								 pgt.str_id_fc,
								 TRUNC (pgt.dtm_fecha_proceso_pago)
									 AS dtm_fecha_proceso_pago,
								 pgt.str_nro_ref_fc
						  FROM admsam.ptr_tbl_archivos arc,
								 admsam.ptr_tbl_pagos_tercero pgt
						 WHERE	  arc.str_estado_archivo IN ('ET3')
								 AND pgt.str_estado_pago IN ('D', 'P')
								 AND pgt.num_id_archivo = arc.num_id_archivo
								 AND TRUNC (pgt.dtm_fecha_ingreso_archivo) =
										  TRUNC (arc.dtm_fecha_cargue))
			SELECT z.num_id_pago_tercero,
					 z.str_ciclo,
					 z.str_nro_ref_fc,
					 z.dtm_fecha_cobro
			  FROM (SELECT wpt.num_id_pago_tercero,
								(CASE
									 WHEN mpi.payment_status IN ('A', 'C')
									 THEN
										 mpi.checker_dt_stamp
									 ELSE
										 mpi.pmt_start_date
								 END)
									AS dtm_fecha_cobro,
								(CASE
									 WHEN mpi.cycle NOT IN (1, 2, 3, 4, 5) THEN '1'
									 ELSE mpi.cycle
								 END)
									AS str_ciclo,
								mpi.reference_no AS str_nro_ref_fc
						 FROM admsam.ptr_tbl_pctm_masspy_pymnt_inpt mpi,
								wt_pagos_tercero wpt
						WHERE 	 mpi.list_id = wpt.str_numero_control
								AND mpi.seq_no = wpt.str_id_fc
								AND mpi.payment_status IN ('R')) z;

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCCESS INSERT',
							indescripcionlog		 => 'MARCA ESTADO R',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		log_ejecucion (inestadolog 			 => 'START UPDATE',
							indescripcionlog		 => 'MARCA ESTADO R',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		UPDATE admsam.ptr_tbl_pagos_tercero pgt
			SET (str_estado_pago, dtm_fecha_cobro, str_ciclo, str_nro_ref_fc) =
					 (SELECT 'R',
								et4.dtm_fecha_cobro,
								et4.str_ciclo,
								et4.str_nro_ref_fc
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero)
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero);

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS UPDATE',
							indescripcionlog		 => 'MARCA ESTADO R',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		-- -----------------------------------------------------------------------
		-- PAGOS LINEA PROPIOS EN ESTADO R  - ACTUALIZA REFERENCE_NO
		-- -----------------------------------------------------------------------

		log_ejecucion (
			inestadolog 			 => 'START INSERT',
			indescripcionlog		 => 'ACTUALIZA REFERENCE_NO PAGOS LINEA PROPIOS EN ESTADO R',
			infechainicio			 => SYSDATE,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_ET4',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		INSERT INTO admsam.ptr_tbl_et4 (num_id_pago_tercero, str_nro_ref_fc)
			WITH wt_pagos_tercero
				  AS (SELECT /*+ MATERIALIZE */
								pgt.num_id_pago_tercero,
								 pgt.str_numero_producto_origen,
								 pgt.str_numero_control,
								 pgt.str_id_fc,
								 TRUNC (pgt.dtm_fecha_proceso_pago)
									 AS dtm_fecha_proceso_pago,
								 pgt.str_nro_ref_fc
						  FROM admsam.ptr_tbl_archivos arc,
								 admsam.ptr_tbl_pagos_tercero pgt
						 WHERE	  arc.str_estado_archivo IN ('ET3')
								 AND pgt.str_estado_pago IN ('R')
								 AND pgt.num_id_archivo = arc.num_id_archivo
								 AND str_nro_ref_fc IS NULL
								 AND TRUNC (pgt.dtm_fecha_ingreso_archivo) =
										  TRUNC (arc.dtm_fecha_cargue))
			SELECT z.num_id_pago_tercero, z.str_nro_ref_fc
			  FROM (SELECT wpt.num_id_pago_tercero,
								(CASE
									 WHEN mpi.payment_status IN ('A', 'C')
									 THEN
										 mpi.checker_dt_stamp
									 ELSE
										 mpi.pmt_start_date
								 END)
									AS dtm_fecha_cobro,
								(CASE
									 WHEN mpi.cycle NOT IN (1, 2, 3, 4, 5) THEN '1'
									 ELSE mpi.cycle
								 END)
									AS str_ciclo,
								mpi.reference_no AS str_nro_ref_fc
						 FROM admsam.ptr_tbl_pctm_masspy_pymnt_inpt mpi,
								wt_pagos_tercero wpt
						WHERE 	 mpi.list_id = wpt.str_numero_control
								AND mpi.seq_no = wpt.str_id_fc
								AND mpi.payment_status IN ('R')) z;

		COMMIT;

		log_ejecucion (
			inestadolog 			 => 'SUCCESS INSERT',
			indescripcionlog		 => 'ACTUALIZA REFERENCE_NO PAGOS LINEA PROPIOS EN ESTADO R',
			infechainicio			 => SYSDATE,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		log_ejecucion (
			inestadolog 			 => 'START UPDATE',
			indescripcionlog		 => 'ACTUALIZA REFERENCE_NO PAGOS LINEA PROPIOS EN ESTADO R',
			infechainicio			 => SYSDATE,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		UPDATE admsam.ptr_tbl_pagos_tercero pgt
			SET (str_estado_pago, dtm_fecha_cobro, str_ciclo, str_nro_ref_fc) =
					 (SELECT 'R',
								et4.dtm_fecha_cobro,
								et4.str_ciclo,
								et4.str_nro_ref_fc
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero)
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero);

		COMMIT;

		log_ejecucion (
			inestadolog 			 => 'SUCESS UPDATE',
			indescripcionlog		 => 'ACTUALIZA REFERENCE_NO PAGOS LINEA PROPIOS EN ESTADO R',
			infechainicio			 => SYSDATE,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);
		-- -----------------------------------------------------------------------
		-- Estado A
		-- -----------------------------------------------------------------------

		log_ejecucion (inestadolog 			 => 'START INSERT',
							indescripcionlog		 => 'MARCA ESTADO A',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_ET4',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		INSERT INTO admsam.ptr_tbl_et4 (num_id_pago_tercero,
												  dtm_fecha_anulacion
												 )
			WITH wt_pagos_tercero
				  AS (SELECT /*+ MATERIALIZE */
								pgt.num_id_pago_tercero,
								 pgt.str_numero_control,
								 pgt.str_id_fc
						  FROM admsam.ptr_tbl_archivos arc,
								 admsam.ptr_tbl_pagos_tercero pgt
						 WHERE	  arc.str_estado_archivo IN ('ET3')
								 AND pgt.str_estado_pago IN ('P', 'D')
								 AND pgt.num_id_archivo = arc.num_id_archivo)
			SELECT z.num_id_pago_tercero, z.dtm_fecha_anulacion
			  FROM (SELECT wpt.num_id_pago_tercero,
								(CASE
									 WHEN mpi.payment_status IN ('A', 'C')
									 THEN
										 mpi.checker_dt_stamp
								 END)
									AS dtm_fecha_anulacion
						 FROM admsam.ptr_tbl_pctm_masspy_pymnt_inpt mpi,
								wt_pagos_tercero wpt
						WHERE 	 mpi.list_id = wpt.str_numero_control
								AND mpi.seq_no = wpt.str_id_fc
								AND mpi.payment_status IN ('A')) z;

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS INSERT',
							indescripcionlog		 => 'MARCA ESTADO A',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		log_ejecucion (inestadolog 			 => 'START UPDATE',
							indescripcionlog		 => 'MARCA ESTADO A',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		UPDATE admsam.ptr_tbl_pagos_tercero pgt
			SET (str_estado_pago,
				  dtm_fecha_anulacion_pago,
				  str_tipo_comision,
				  num_valor_iva,
				  num_valor_comision
				 ) =
					 (SELECT 'A', et4.dtm_fecha_anulacion, NULL, 0, 0
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero)
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero);

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS UPDATE',
							indescripcionlog		 => 'MARCA ESTADO A',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		-- -----------------------------------------------------------------------
		-- Estado E
		-- -----------------------------------------------------------------------

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_ET4',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		log_ejecucion (inestadolog 			 => 'SUCESS INSERT',
							indescripcionlog		 => 'MARCA ESTADO E',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		INSERT INTO admsam.ptr_tbl_et4 (num_id_pago_tercero,
												  dtm_fecha_anulacion
												 )
			WITH wt_pagos_tercero
				  AS (SELECT /*+ MATERIALIZE */
								pgt.num_id_pago_tercero,
								 pgt.str_numero_control,
								 pgt.str_id_fc
						  FROM admsam.ptr_tbl_archivos arc,
								 admsam.ptr_tbl_pagos_tercero pgt
						 WHERE	  arc.str_estado_archivo IN ('ET3')
								 AND pgt.str_estado_pago IN ('P', 'D')
								 AND pgt.num_id_archivo = arc.num_id_archivo)
			SELECT z.num_id_pago_tercero, z.dtm_fecha_anulacion
			  FROM (SELECT wpt.num_id_pago_tercero,
								(CASE
									 WHEN mpi.payment_status IN ('A', 'C')
									 THEN
										 mpi.checker_dt_stamp
								 END)
									AS dtm_fecha_anulacion
						 FROM admsam.ptr_tbl_pctm_masspy_pymnt_inpt mpi,
								wt_pagos_tercero wpt
						WHERE 	 mpi.list_id = wpt.str_numero_control
								AND mpi.seq_no = wpt.str_id_fc
								AND mpi.payment_status IN ('E')) z;

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS INSERT',
							indescripcionlog		 => 'MARCA ESTADO E',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		log_ejecucion (inestadolog 			 => 'START UPDATE',
							indescripcionlog		 => 'MARCA ESTADO E',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		UPDATE admsam.ptr_tbl_pagos_tercero pgt
			SET (str_estado_pago,
				  dtm_fecha_anulacion_pago,
				  str_tipo_comision,
				  num_valor_iva,
				  num_valor_comision
				 ) =
					 (SELECT 'E', et4.dtm_fecha_anulacion, NULL, 0, 0
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero)
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero);

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS UPDATE',
							indescripcionlog		 => 'MARCA ESTADO A',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		-- -----------------------------------------------------------------------
		-- Estado L
		-- -----------------------------------------------------------------------

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_ET4',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		log_ejecucion (inestadolog 			 => 'SUCESS INSERT',
							indescripcionlog		 => 'MARCA ESTADO L',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		INSERT INTO admsam.ptr_tbl_et4 (num_id_pago_tercero,
												  dtm_fecha_anulacion
												 )
			WITH wt_pagos_tercero
				  AS (SELECT /*+ MATERIALIZE */
								pgt.num_id_pago_tercero,
								 pgt.str_numero_control,
								 pgt.str_id_fc
						  FROM admsam.ptr_tbl_archivos arc,
								 admsam.ptr_tbl_pagos_tercero pgt
						 WHERE	  arc.str_estado_archivo IN ('ET3')
								 AND pgt.str_estado_pago IN ('P', 'D')
								 AND pgt.num_id_archivo = arc.num_id_archivo)
			SELECT z.num_id_pago_tercero, z.dtm_fecha_anulacion
			  FROM (SELECT wpt.num_id_pago_tercero,
								(CASE
									 WHEN mpi.payment_status IN ('A', 'C')
									 THEN
										 mpi.checker_dt_stamp
								 END)
									AS dtm_fecha_anulacion
						 FROM admsam.ptr_tbl_pctm_masspy_pymnt_inpt mpi,
								wt_pagos_tercero wpt
						WHERE 	 mpi.list_id = wpt.str_numero_control
								AND mpi.seq_no = wpt.str_id_fc
								AND mpi.payment_status IN ('L')) z;

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS INSERT',
							indescripcionlog		 => 'MARCA ESTADO L',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		log_ejecucion (inestadolog 			 => 'START UPDATE',
							indescripcionlog		 => 'MARCA ESTADO L',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		UPDATE admsam.ptr_tbl_pagos_tercero pgt
			SET (str_estado_pago,
				  dtm_fecha_anulacion_pago,
				  str_tipo_comision,
				  num_valor_iva,
				  num_valor_comision
				 ) =
					 (SELECT 'L', et4.dtm_fecha_anulacion, NULL, 0, 0
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero)
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero);

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS UPDATE',
							indescripcionlog		 => 'MARCA ESTADO L',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );


		-- -----------------------------------------------------------------------
		-- Estado C
		-- -----------------------------------------------------------------------

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_ET4',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		log_ejecucion (inestadolog 			 => 'START INSERT',
							indescripcionlog		 => 'MARCA ESTADO C',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		INSERT INTO admsam.ptr_tbl_et4 (num_id_pago_tercero,
												  dtm_fecha_anulacion
												 )
			WITH wt_pagos_tercero
				  AS (SELECT /*+ MATERIALIZE */
								pgt.num_id_pago_tercero,
								 pgt.str_numero_control,
								 pgt.str_id_fc
						  FROM admsam.ptr_tbl_archivos arc,
								 admsam.ptr_tbl_pagos_tercero pgt
						 WHERE	  arc.str_estado_archivo IN ('ET3')
								 AND pgt.str_estado_pago IN ('P', 'D')
								 AND pgt.num_id_archivo = arc.num_id_archivo)
			SELECT z.num_id_pago_tercero, z.dtm_fecha_anulacion
			  FROM (SELECT wpt.num_id_pago_tercero,
								(CASE
									 WHEN mpi.payment_status IN ('A', 'C')
									 THEN
										 mpi.checker_dt_stamp
								 END)
									AS dtm_fecha_anulacion
						 FROM admsam.ptr_tbl_pctm_masspy_pymnt_inpt mpi,
								wt_pagos_tercero wpt
						WHERE 	 mpi.list_id = wpt.str_numero_control
								AND mpi.seq_no = wpt.str_id_fc
								AND mpi.payment_status IN ('C')) z;

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS INSERT',
							indescripcionlog		 => 'MARCA ESTADO C',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		log_ejecucion (inestadolog 			 => 'START UPDATE',
							indescripcionlog		 => 'MARCA ESTADO C',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		UPDATE admsam.ptr_tbl_pagos_tercero pgt
			SET (str_estado_pago,
				  dtm_fecha_anulacion_pago,
				  str_tipo_comision,
				  num_valor_iva,
				  num_valor_comision
				 ) =
					 (SELECT 'C', et4.dtm_fecha_anulacion, NULL, 0, 0
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero)
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero);

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS UPDATE',
							indescripcionlog		 => 'MARCA ESTADO C',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		-- -----------------------------------------------------------------------
		-- IVA y COMISION Estado D y R
		-- -----------------------------------------------------------------------

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_ET4',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		log_ejecucion (inestadolog 			 => 'START INSERT',
							indescripcionlog		 => 'IVA Y COMISION',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		INSERT
       INTO ADMSAM.PTR_TBL_ET4
            (
             NUM_ID_PAGO_TERCERO,
             STR_TIPO_COMISION,
             NUM_VALOR_IVA,
             NUM_VALOR_COMISION
            )
            WITH WT_PAGOS_TERCERO AS (SELECT /*+ MATERIALIZE */
                                 PGT.NUM_ID_PAGO_TERCERO,
                                 PGT.STR_NUMERO_PRODUCTO_ORIGEN,
                                 PGT.STR_NUMERO_CONTROL,
                                 PGT.STR_ID_FC,
                                 PGT.STR_NRO_REF_FC,
                                 TRUNC(PGT.DTM_FECHA_PROCESO_PAGO) AS DTM_FECHA_PROCESO_PAGO
                            FROM ADMSAM.PTR_TBL_ARCHIVOS ARC,
                                 ADMSAM.PTR_TBL_PAGOS_TERCERO PGT
                           WHERE ARC.STR_ESTADO_ARCHIVO IN ('ET3')
                             AND PGT.NUM_ID_ARCHIVO = ARC.NUM_ID_ARCHIVO
                             AND PGT.STR_ESTADO_PAGO  IN ('R', 'D')
                             AND PGT.STR_MODO_PAGO IN ('2','3','5')
                             and pgt.num_valor_iva is null
                             and pgt.num_valor_comision is null
                             AND PGT.DTM_FECHA_PROCESO_PAGO >= TRUNC(SYSDATE-10)
                           UNION ALL
                          SELECT /*+ MATERIALIZE */
                                 PGT.NUM_ID_PAGO_TERCERO,
                                 PGT.STR_NUMERO_PRODUCTO_ORIGEN,
                                 PGT.STR_NUMERO_CONTROL,
                                 PGT.STR_ID_FC,
                                 PGT.STR_NRO_REF_FC,
                                 TRUNC(PGT.DTM_FECHA_PROCESO_PAGO) AS DTM_FECHA_PROCESO_PAGO
                            FROM ADMSAM.PTR_TBL_ARCHIVOS ARC,
                                 ADMSAM.PTR_TBL_PAGOS_TERCERO PGT
                           WHERE ARC.STR_ESTADO_ARCHIVO IN ('ET3')
                             AND PGT.NUM_ID_ARCHIVO = ARC.NUM_ID_ARCHIVO
                             AND PGT.STR_ESTADO_PAGO  IN ('R', 'D')
                             AND PGT.STR_MODO_PAGO IN ('1','4')
                             and pgt.num_valor_iva is null
                             and pgt.num_valor_comision is null)
SELECT z.NUM_ID_PAGO_TERCERO,
       max(z.TIPO_COMISION),
       sum(z.IVA),
       sum(z.COMISION)
  FROM (SELECT WPT.NUM_ID_PAGO_TERCERO,
               decode(ENT.AMOUNT_TAG, 'CHG_AMT1', nvl(TCD.TRN_DESC, ' '), ' ') tipo_comision,
               SUM(decode(ent.TRN_CODE, 'A13', nvl(ENT.LCY_AMOUNT, 0), 0)) AS IVA,
               SUM(decode(ent.TRN_CODE, 'A13', 0, 'BZ3', 0, nvl(ENT.LCY_AMOUNT, 0))) AS comision
          FROM ADMSAM.PTR_TBL_PCTM_MASSPY_PYMNT_INPT MPI,
               WT_PAGOS_TERCERO WPT,
               ADMSAM.PTR_TBL_ACVW_ALL_AC_ENTRIES ENT,
               FCJADM.STTM_TRN_CODE@opcbsp TCD
         WHERE MPI.LIST_ID = WPT.STR_NUMERO_CONTROL
           AND MPI.SEQ_NO = WPT.STR_ID_FC
           AND MPI.PAYMENT_STATUS IN ('D','R')
           and ENT.AC_NO(+) = WPT.STR_NUMERO_PRODUCTO_ORIGEN
           AND ENT.DRCR_IND(+) IN ('D')
           AND ENT.TRN_DT(+) =  WPT.DTM_FECHA_PROCESO_PAGO
           AND ENT.TRN_REF_NO(+) = WPT.STR_NRO_REF_FC
           AND ENT.TRN_CODE(+) IN ('A13', 'BZ0', 'BZ2', 'C63', 'C64', 'C65', 'C68', 'BZ1', 'C66', '255', '251', '246', 'BZ4', 'BZ3')
           AND ENT.TRN_CODE = TCD.TRN_CODE (+)
         GROUP BY WPT.NUM_ID_PAGO_TERCERO, decode(ENT.AMOUNT_TAG, 'CHG_AMT1', nvl(TCD.TRN_DESC, ' '), ' ')) z group by z.NUM_ID_PAGO_TERCERO;

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS INSERT',
							indescripcionlog		 => 'IVA Y COMISION',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		log_ejecucion (inestadolog 			 => 'START UPDATE',
							indescripcionlog		 => 'IVA Y COMISION',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		UPDATE admsam.ptr_tbl_pagos_tercero pgt
			SET (str_tipo_comision, num_valor_iva, num_valor_comision) =
					 (SELECT et4.str_tipo_comision,
								et4.num_valor_iva,
								et4.num_valor_comision
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero)
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero);

		COMMIT;

		log_ejecucion (inestadolog 			 => 'SUCESS UPDATE',
							indescripcionlog		 => 'IVA Y COMISION',
							infechainicio			 => SYSDATE,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );


		-- -----------------------------------------------------------------------------
		-- IVA y COMISION PAGOS EN LINEA PROPIOS QUE QUEDAN DE INMEDIATO EN ESTADO R
		-- ------------------------------------------------------------------------------

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_ET4',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		log_ejecucion (
			inestadolog 			 => 'START INSERT',
			indescripcionlog		 => 'IVA Y COMISION PAGOS EN LINEA PROPIOS QUE QUEDAN DE INMEDIATO EN ESTADO R',
			infechainicio			 => SYSDATE,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		INSERT INTO admsam.ptr_tbl_et4 (num_id_pago_tercero,
												  str_tipo_comision,
												  num_valor_iva,
												  num_valor_comision
												 )
			WITH wt_pagos_tercero
				  AS (SELECT /*+ MATERIALIZE */
								pgt.num_id_pago_tercero,
								 pgt.str_numero_producto_origen,
								 pgt.str_numero_control,
								 pgt.str_id_fc,
								 pgt.str_nro_ref_fc,
								 TRUNC (pgt.dtm_fecha_proceso_pago)
									 AS dtm_fecha_proceso_pago
						  FROM admsam.ptr_tbl_archivos arc,
								 admsam.ptr_tbl_pagos_tercero pgt
						 WHERE	  pgt.num_id_archivo = arc.num_id_archivo
								 AND pgt.str_estado_pago IN ('R')
								 AND arc.str_tipo_archivo IN ('PGL')
								 AND arc.str_estado_archivo IN ('ET3')
								 AND pgt.dtm_fecha_proceso_pago >= TRUNC (SYSDATE)
                                 and pgt.num_valor_iva is null
                                 and pgt.num_valor_comision is null),
				  wt_entries
				  AS (SELECT /*+ MATERIALIZE */
								wpt.str_numero_producto_origen,
								 ent.trn_dt,
								 ent.trn_ref_no,
								 ent.trn_code
						  FROM admsam.ptr_tbl_acvw_all_ac_entries ent,
								 wt_pagos_tercero wpt
						 WHERE	  ent.drcr_ind IN ('D')
								 AND ent.amount_tag IN ('CHG_AMT1')
								 AND ent.trn_code NOT IN ('BZ3')
								 AND ent.ac_no = wpt.str_numero_producto_origen
								 AND ent.trn_ref_no = wpt.str_nro_ref_fc),
				  wt_lcy_amount
				  AS (SELECT /*+ MATERIALIZE */
								ent.trn_ref_no,
									ent.trn_code,
									SUM (ent.lcy_amount) AS lcy_amount
							 FROM admsam.ptr_tbl_acvw_all_ac_entries ent,
									wt_pagos_tercero wpt
							WHERE 	 ent.ac_no = wpt.str_numero_producto_origen
									AND ent.drcr_ind IN ('D')
									AND ent.trn_dt = wpt.dtm_fecha_proceso_pago
									AND ent.trn_ref_no = wpt.str_nro_ref_fc
									AND ent.trn_code IN
											 ('A13',
											  'BZ0',
											  'BZ2',
											  'C63',
											  'C64',
											  'C65',
											  'C68',
											  'BZ1',
											  'C66',
											  '255',
											  '251',
											  '246',
											  'BZ4')
						GROUP BY ent.trn_ref_no, ent.trn_code)
			SELECT z.num_id_pago_tercero,
					 z.tipo_comision,
					 z.iva,
					 (z.comision - z.iva)
			  FROM (SELECT wpt.num_id_pago_tercero,
								NVL (
									(SELECT tcd.trn_desc
										FROM fcjadm.sttm_trn_code@opcbsp tcd,
											  wt_entries ent
									  WHERE		ent.trn_dt = mpi.due_date
											  AND ent.trn_ref_no = mpi.reference_no
											  AND ent.trn_code = tcd.trn_code),
									' ')
									AS tipo_comision,
								(SELECT NVL (SUM (lcy_amount), 0) AS lcy_amount
									FROM wt_lcy_amount
								  WHERE		trn_ref_no = mpi.reference_no
										  AND trn_code IN ('A13'))
									AS iva,
								(SELECT NVL (SUM (lcy_amount), 0) AS lcy_amount
									FROM wt_lcy_amount
								  WHERE		trn_ref_no = mpi.reference_no
										  AND trn_code IN
													('A13',
													 'BZ0',
													 'BZ2',
													 'C63',
													 'C64',
													 'C65',
													 'C68',
													 'BZ1',
													 'C66',
													 '255',
													 '251',
													 '246',
													 'BZ4'))
									AS comision
						 FROM admsam.ptr_tbl_pctm_masspy_pymnt_inpt mpi,
								wt_pagos_tercero wpt
						WHERE 	 mpi.list_id = wpt.str_numero_control
								AND mpi.seq_no = wpt.str_id_fc
								AND mpi.payment_status IN ('R')) z;

		COMMIT;

		log_ejecucion (
			inestadolog 			 => 'SUCESS INSERT',
			indescripcionlog		 => 'IVA Y COMISION PAGOS EN LINEA PROPIOS QUE QUEDAN DE INMEDIATO EN ESTADO R',
			infechainicio			 => SYSDATE,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		log_ejecucion (
			inestadolog 			 => 'START UPDATE',
			indescripcionlog		 => 'IVA Y COMISION PAGOS EN LINEA PROPIOS QUE QUEDAN DE INMEDIATO EN ESTADO R',
			infechainicio			 => SYSDATE,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		UPDATE admsam.ptr_tbl_pagos_tercero pgt
			SET (str_tipo_comision, num_valor_iva, num_valor_comision) =
					 (SELECT et4.str_tipo_comision,
								et4.num_valor_iva,
								et4.num_valor_comision
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero)
		 WHERE EXISTS
					 (SELECT NULL
						 FROM admsam.ptr_tbl_et4 et4
						WHERE et4.num_id_pago_tercero = pgt.num_id_pago_tercero);

		COMMIT;

		log_ejecucion (
			inestadolog 			 => 'SUCESS UPDATE',
			indescripcionlog		 => 'IVA Y COMISION PAGOS EN LINEA PROPIOS QUE QUEDAN DE INMEDIATO EN ESTADO R',
			infechainicio			 => SYSDATE,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);

		log_ejecucion (inestadolog 			 => 'END',
							indescripcionlog		 => 'ETAPA 4, MARCA ESTADOS',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'ETAPA 4 MARCA ESTADOS, ERROR:  '
												 || SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_cuatro_marca_estados;

    PROCEDURE etapa_cuatro_actu_fech_pagos (
        infechainicio         IN    DATE,
        outestadoejecucion    OUT   PLS_INTEGER,
        outmensajeejecucion   OUT   VARCHAR2
    ) IS
    BEGIN
        dbms_application_info.set_module(module_name => 'ptr_pkg_sincronizar_pagos', action_name => 'etapa_cuatro_actu_fech_pagos'
        );
        log_ejecucion(inestadolog => 'START', indescripcionlog => 'ETAPA 4, ACTUALIZAR FECHA PROCESO PAGOS', infechainicio => infechainicio
        , outestadoejecucion => outestadoejecucion, outmensajeejecucion => outmensajeejecucion);

        IF outestadoejecucion <> estado_ejecucion_exitosa THEN
            RAISE excepcion_personalizada;
        END IF;

        admsam.gbl_pkg_ddl.truncar_tabla(innombretabla => 'PTR_TBL_ET4', outestadoejecucion => outestadoejecucion, outmensajeejecucion
        => outmensajeejecucion);

        IF outestadoejecucion <> estado_ejecucion_exitosa THEN
            RAISE excepcion_personalizada;
        END IF;

        log_ejecucion(inestadolog => 'START', indescripcionlog => 'ACTUALIZAR FECHA PROCESO PAGOS', infechainicio => sysdate, outestadoejecucion
        => outestadoejecucion, outmensajeejecucion => outmensajeejecucion);

        INSERT INTO admsam.ptr_tbl_et4 (
            num_id_pago_tercero,
            dtm_fecha_cobro
        )
            SELECT
                pgt.num_id_pago_tercero,
                pfc.due_date
            FROM
                admsam.ptr_tbl_pctm_masspy_pymnt_inpt   pfc,
                admsam.ptr_tbl_archivos                 arc,
                admsam.ptr_tbl_pagos_tercero            pgt
            WHERE
                pgt.num_id_archivo = arc.num_id_archivo
                AND arc.str_estado_archivo = 'ET3'
                AND arc.str_tipo_archivo IN (
                    'PGO',
                    'PGL'
                )
                AND pfc.seq_no = pgt.str_id_fc
                AND pfc.due_date > trunc(sysdate);

        COMMIT;
        log_ejecucion(inestadolog => 'SUCESS', indescripcionlog => 'ACTUALIZAR FECHA PROCESO PAGOS', infechainicio => sysdate, outestadoejecucion
        => outestadoejecucion, outmensajeejecucion => outmensajeejecucion);

        log_ejecucion(inestadolog => 'START UPDATE', indescripcionlog => 'ACTUALIZAR FECHA PROCESO PAGOS', infechainicio => sysdate
        , outestadoejecucion => outestadoejecucion, outmensajeejecucion => outmensajeejecucion);

        UPDATE admsam.ptr_tbl_pagos_tercero pgt
        SET
            ( pgt.dtm_fecha_proceso_pago ) = (
                SELECT
                    te4.dtm_fecha_cobro
                FROM
                    admsam.ptr_tbl_et4 te4
                WHERE
                    te4.num_id_pago_tercero = pgt.num_id_pago_tercero
            )
        WHERE
            EXISTS (
                SELECT
                    NULL
                FROM
                    admsam.ptr_tbl_et4 te4
                WHERE
                    te4.num_id_pago_tercero = pgt.num_id_pago_tercero
            );

        COMMIT;
        log_ejecucion(inestadolog => 'SUCCESS UPDATE', indescripcionlog => 'ACTUALIZAR FECHA PROCESO PAGOS', infechainicio => sysdate
        , outestadoejecucion => outestadoejecucion, outmensajeejecucion => outmensajeejecucion);

        log_ejecucion(inestadolog => 'END', indescripcionlog => 'ETAPA 4, ACTUALIZAR FECHA PROCESO PAGOS', infechainicio => infechainicio
        , outestadoejecucion => outestadoejecucion, outmensajeejecucion => outmensajeejecucion);

        IF outestadoejecucion <> estado_ejecucion_exitosa THEN
            RAISE excepcion_personalizada;
        END IF;
        outestadoejecucion := estado_ejecucion_exitosa;
    EXCEPTION
        WHEN OTHERS THEN
            log_ejecucion(inestadolog => sqlcode, indescripcionlog => 'ETAPA 4 ACTUALIZAR FECHA PROCESO PAGOS, ERROR:  '
                                                                      || substr(sqlerrm, 1, 200)
                                                                      || dbms_utility.format_error_backtrace, infechainicio => infechainicio
                                                                      , outestadoejecucion => outestadoejecucion, outmensajeejecucion
                                                                      => outmensajeejecucion);

            outestadoejecucion := sqlcode;
            outmensajeejecucion := substr(sqlerrm, 1, 200)
                                   || dbms_utility.format_error_backtrace;
    END etapa_cuatro_actu_fech_pagos;

	PROCEDURE etapa_cuatro_actualiza_contdrs (
		infechainicio			 IN	  DATE,
		outestadoejecucion		 OUT PLS_INTEGER,
		outmensajeejecucion		 OUT VARCHAR2)
	IS
	BEGIN
	    DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_cuatro_actualiza_contdrs');

		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'ETAPA 4, ACTUALIZA CONTADORES',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

        UPDATE admsam.ptr_tbl_archivos arc
        SET
            ( arc.num_total_registros_exitosos,
              arc.num_total_registros_rechazados,
              arc.num_total_registros_en_proceso,
              arc.num_valor_suma_reg_exitosos,
              arc.num_valor_suma_reg_rechazados ) = (
                SELECT
                    SUM(
                        CASE
                            WHEN ptr.str_estado_pago IN(
                                'R'
                            ) THEN
                                1
                            ELSE
                                0
                        END
                    ) AS num_total_registros_exitosos,
                    SUM(
                        CASE
                            WHEN ptr.str_estado_pago IN(
                                'X'
                            ) THEN
                                1
                            ELSE
                                0
                        END
                    ) AS num_total_registros_rechazados,
                    SUM(
                        CASE
                            WHEN ptr.str_estado_pago IN(
                                'D', 'P'
                            ) THEN
                                1
                            ELSE
                                0
                        END
                    ) AS num_total_registros_en_proceso,
                    SUM(
                        CASE
                            WHEN ptr.str_estado_pago IN(
                                'R'
                            ) THEN
                                ptr.num_valor_pago
                            ELSE
                                0
                        END
                    ) AS num_valor_suma_reg_exitosos,
                    SUM(
                        CASE
                            WHEN ptr.str_estado_pago IN(
                                'X'
                            ) THEN
                                ptr.num_valor_pago
                            ELSE
                                0
                        END
                    ) AS num_valor_suma_reg_rechazados
                FROM
                    admsam.ptr_tbl_pagos_tercero ptr
                WHERE
                    ptr.num_id_archivo = arc.num_id_archivo
            )
        WHERE
            (arc.str_estado_archivo IN (
                'ENV',
                'ET1',
                'ET2',
                'ET3'
            )) OR
            (arc.str_estado_archivo = 'RCH'
            and trunc(arc.dtm_fecha_cargue) >= trunc(sysdate));

		log_ejecucion (inestadolog 			 => 'END',
							indescripcionlog		 => 'ETAPA 4, ACTUALIZA CONTADORES',
							infechainicio			 => infechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'ETAPA 4 ACTUALIZA CONTADORES, ERROR:  '
												 || SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => infechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_cuatro_actualiza_contdrs;

  	PROCEDURE etapa_cuatro (outestadoejecucion	 OUT PLS_INTEGER,
									outmensajeejecucion	 OUT VARCHAR2
								  )
	IS
		fechainicio   DATE DEFAULT SYSDATE;
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_cuatro');
		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'INICIO ETAPA 4',
							infechainicio			 => fechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		--DBMS_OUTPUT.put_line ('paso 1');

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		carga_entries (outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );
		--DBMS_OUTPUT.put_line ('paso 2');

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		admsam.gbl_pkg_ddl.truncar_tabla (
			innombretabla			 => 'PTR_TBL_PCTM_MASSPY_PYMNT_INPT',
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);
		--DBMS_OUTPUT.put_line ('paso 3');
		
		--06/03/23 GORTIZ: Insert en admsam.ptr_tbl_pctm_masspy_pymnt_inpt informacion de pagos por carga de archivos
		INSERT INTO admsam.ptr_tbl_pctm_masspy_pymnt_inpt (payment_status,
																			err_code,
																			error_reason,
																			list_id,
																			seq_no,
																			reference_no,
																			due_date,
																			pmt_start_date, --IM799804
																			checker_dt_stamp,  --IM799804
                                                                            cycle
																		  )
			SELECT mpi.payment_status,
					 mpi.err_code,
					 mpi.error_reason,
					 mpi.list_id,
					 mpi.seq_no,
					 CASE WHEN mpi.payment_mode = 'EAC' THEN
						mpi.pc_trn_ref_no
					 ELSE
						mpi.reference_no
					 END,
					 mpi.due_date,
					 mpi.pmt_start_date, --IM799804
					 mpi.checker_dt_stamp, --IM799804
                                     (
                    CASE
                        WHEN length(TRIM(mpi.cycle)) = 1 THEN
                            mpi.cycle
                        ELSE
                            '1'
                    END
                ) AS ciclo
			  FROM fcjadm.pctm_masspay_payment_input@opcbsp mpi,
                   admsam.ptr_tbl_archivos arh
			WHERE
                mpi.file_name = arh.str_nombre_archivo
                AND arh.str_estado_archivo IN (
                    'ET2',
                    'ET3'
                )
                AND arh.str_tipo_archivo = 'PGO'; 

				
			--06/03/23 GORTIZ: Insert en admsam.ptr_tbl_pctm_masspy_pymnt_inpt informacion de pagos en Linea
			INSERT INTO admsam.ptr_tbl_pctm_masspy_pymnt_inpt (payment_status,
																	err_code,
																	error_reason,
																	list_id,
																	seq_no,
																	reference_no,
																	due_date,
																	pmt_start_date, --IM799804
																	checker_dt_stamp,  --IM799804
																	cycle
																  )
            SELECT mpi.payment_status,
					 mpi.err_code,
					 mpi.error_reason,
					 mpi.list_id,
					 pgo.STR_ID_FC,
					 CASE WHEN mpi.payment_mode = 'EAC' THEN
						mpi.pc_trn_ref_no
					 ELSE
						mpi.reference_no
					 END,
					 mpi.due_date,
					 mpi.pmt_start_date, --IM799804
					 mpi.checker_dt_stamp, --IM799804
                                     (
                    CASE
                        WHEN length(TRIM(mpi.cycle)) = 1 THEN
                            mpi.cycle
                        ELSE
                            '1'
                    END
                ) AS ciclo
                 FROM
                fcjadm.pctm_masspay_payment_input@opcbsp   mpi,
                admsam.ptr_tbl_archivos                         arh,
                admsam.ptr_tbl_pagos_tercero                    pgo
            WHERE
                pgo.num_id_archivo = arh.num_id_archivo
                AND arh.str_estado_archivo = 'ET3'
                AND arh.str_tipo_archivo = 'PGL'
				AND mpi.list_id = pgo.STR_NUMERO_CONTROL;


             --WHERE mpi.payment_status IN ('A', 'C', 'D', 'E', 'R', 'X', 'L');

		--DBMS_OUTPUT.put_line ('paso 4');
		etapa_cuatro_rechazados (infechainicio 		  => fechainicio,
										 outestadoejecucion	  => outestadoejecucion,
										 outmensajeejecucion   => outmensajeejecucion
										);
		--DBMS_OUTPUT.put_line ('paso 5');

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		etapa_cuatro_marca_estados (infechainicio 		  => fechainicio,
											 outestadoejecucion	  => outestadoejecucion,
											 outmensajeejecucion   => outmensajeejecucion
											);
		--DBMS_OUTPUT.put_line ('paso 6');

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		etapa_cuatro_actu_fech_pagos (
			infechainicio			 => fechainicio,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);
		--DBMS_OUTPUT.put_line ('paso 7');


		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		etapa_cuatro_actualiza_contdrs (
			infechainicio			 => fechainicio,
			outestadoejecucion	 => outestadoejecucion,
			outmensajeejecucion	 => outmensajeejecucion);
		--DBMS_OUTPUT.put_line ('paso 7');

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		log_ejecucion (inestadolog 			 => 'END',
							indescripcionlog		 => 'FIN ETAPA 4',
							infechainicio			 => fechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );
		--DBMS_OUTPUT.put_line ('paso 8 ***');

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN excepcion_personalizada
		THEN
			NULL;
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'FINAL '
												 || SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => fechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_cuatro;

	PROCEDURE etapa_cinco (outestadoejecucion 	OUT PLS_INTEGER,
								  outmensajeejecucion	OUT VARCHAR2
								 )
	IS
		fechainicio   DATE DEFAULT SYSDATE;
	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'etapa_cinco');
		log_ejecucion (inestadolog 			 => 'START',
							indescripcionlog		 => 'ETAPA 5',
							infechainicio			 => fechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

---------------------Cierre Para Archivos-----------------------------------------------------
        UPDATE admsam.ptr_tbl_archivos arc
        SET
            arc.str_estado_archivo = 'EXT'
        WHERE
            arc.str_tipo_archivo = 'PGO'
            AND arc.str_estado_archivo IN (
                'ENV',
                'ET3'
            )
            AND NOT EXISTS (
                SELECT
                    NULL
                FROM
                    admsam.ptr_tbl_pagos_tercero pgt
             WHERE
                    pgt.num_id_archivo = arc.num_id_archivo
                    AND pgt.str_estado_pago IN (
                        'P',
                        'D'
                 )
         );

        COMMIT;

---------------------Cierre Para Pagos en Linea-------------------------------------------------
        UPDATE admsam.ptr_tbl_archivos arc
        SET
            arc.str_estado_archivo = 'EXT'
        WHERE
            arc.str_tipo_archivo = 'PGL'
            AND arc.str_estado_archivo = 'ET3'
            AND EXISTS (
                SELECT
                    NULL
                FROM
                    admsam.ptr_tbl_pagos_tercero pgt
                WHERE
                    pgt.num_id_archivo = arc.num_id_archivo
                    AND pgt.str_nro_ref_fc IS NOT NULL
                    AND pgt.str_estado_pago = 'R'
            );

        COMMIT;


		UPDATE admsam.ptr_tbl_archivos arc
        SET
            arc.str_estado_archivo = 'EXT'
        WHERE
            arc.str_tipo_archivo = 'PGL'
            AND arc.str_estado_archivo = 'ET3'
            AND NOT EXISTS (
                SELECT
                    NULL
                FROM
                    admsam.ptr_tbl_pagos_tercero pgt
             WHERE
                    pgt.num_id_archivo = arc.num_id_archivo
                    AND pgt.str_estado_pago IN (
                        'P',
                        'D',
						'R'
                 )
         );

        COMMIT;

		log_ejecucion (inestadolog 			 => 'END',
							indescripcionlog		 => 'FIN ETAPA 5',
							infechainicio			 => fechainicio,
							outestadoejecucion	 => outestadoejecucion,
							outmensajeejecucion	 => outmensajeejecucion
						  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		outestadoejecucion := estado_ejecucion_exitosa;
	EXCEPTION
		WHEN excepcion_personalizada
		THEN
			NULL;
		WHEN OTHERS
		THEN
			log_ejecucion (
				inestadolog 			 => SQLCODE,
				indescripcionlog		 =>	 'ETAPA 5 MARCA ARCHIVO, ERROR:  '
												 || SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => fechainicio,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END etapa_cinco;

    /* Descripción: Prenota o Prevalidación Beneficiarios */
    PROCEDURE prevalidacion_beneficiario (
        outestadoejecucion    OUT   PLS_INTEGER,
        outmensajeejecucion   OUT   VARCHAR2
    ) IS

    CURSOR c_prevalidacion IS
          SELECT ben.num_id_beneficiarios as ID_BEN,
         (CASE pag.str_estado_pago
          WHEN 'R' THEN 'APR'
          WHEN 'X' THEN 'REC' END) as ESTADO,
         pag.str_codigo_error as COD_ERROR,
         pag.str_descripcion_error as DESC_ERROR
          FROM admsam.ptr_tbl_beneficiarios ben,
         admsam.ptr_tbl_archivos arc,
         admsam.ptr_tbl_pagos_tercero pag
          WHERE ben.str_estado_prevalidacion = 'PEN'
      and pag.str_estado_pago IN ('X', 'R')
      and ben.STR_NOMBRE_ARCHIVO = arc.str_nombre_archivo
      and arc.num_id_archivo = pag.num_id_archivo
      and pag.str_ident_beneficiario = ben.str_identificacion
      and pag.str_codigo_producto_destino = ben.str_codigo_producto
      and pag.str_numero_producto_destino = ben.str_numero_producto;

    TYPE data_type IS TABLE OF c_prevalidacion%rowtype INDEX BY PLS_INTEGER;
    data_tab       data_type;

    BEGIN
    dbms_application_info.set_module(module_name => 'ptr_pkg_sincronizar_pagos', action_name => 'prevalidacion_beneficiario');

        log_ejecucion(inestadolog => 'START', indescripcionlog => 'Inicia Verificación - Estado prevalidación Beneficiario'
        , infechainicio => sysdate, outestadoejecucion => outestadoejecucion, outmensajeejecucion => outmensajeejecucion);

    OPEN c_prevalidacion;
    FETCH c_prevalidacion BULK COLLECT INTO DATA_TAB LIMIT 5000;

    FORALL i IN DATA_TAB.FIRST..DATA_TAB.LAST
        UPDATE admsam.ptr_tbl_beneficiarios
      SET str_estado_prevalidacion = DATA_TAB(i).ESTADO,
              str_codigo_error = DATA_TAB(i).COD_ERROR,
              str_descripcion_error = DATA_TAB(i).DESC_ERROR
        WHERE num_id_beneficiarios = DATA_TAB(i).ID_BEN;

    COMMIT;

    CLOSE c_prevalidacion;

        log_ejecucion(inestadolog => 'END', indescripcionlog => 'Finaliza Verificación - Estado prevalidación Beneficiario'
        , infechainicio => sysdate, infechafinal => sysdate, outestadoejecucion => outestadoejecucion,
        outmensajeejecucion => outmensajeejecucion);

        outestadoejecucion := estado_ejecucion_exitosa;

      EXCEPTION
        WHEN OTHERS THEN
           -- Código y mensaje del error
           outestadoejecucion := sqlcode;
           outmensajeejecucion := substr(sqlerrm, 1, 200);
    END prevalidacion_beneficiario;


    /* Descripción: Prenota o Prevalidación Beneficiarios - Procesamiento dias habiles */
    PROCEDURE beneficiario_dias_habiles (
        outestadoejecucion    OUT   PLS_INTEGER,
        outmensajeejecucion   OUT   VARCHAR2
    ) IS
      marca_inicial   TIMESTAMP := systimestamp;
      marca_final     TIMESTAMP;
      fecha_inicio    DATE DEFAULT sysdate;
      fecha_habil     DATE;


    BEGIN
        dbms_application_info.set_module(module_name => 'ptr_pkg_sincronizar_pagos', action_name => 'prevalidacion_beneficiario');
        fecha_habil := ADMSAM_PR.GBL_PKG_UTILITARIOS.RESTAR_DIAS_HABILES(SYSDATE, 3);
        log_ejecucion(inestadolog => 'Proceso prenota Inicia: ' || marca_inicial, indescripcionlog => 'Inicia Verificación Días Hábiles - Estado prevalidación Beneficiario'
        , infechainicio => fecha_inicio, outestadoejecucion => outestadoejecucion, outmensajeejecucion => outmensajeejecucion);

        UPDATE admsam.ptr_tbl_beneficiarios tb
          SET tb.str_estado_prevalidacion = 'REC',
          tb.str_codigo_error = '',
          tb.str_descripcion_error = 'La prevalidacion excedio el tiempo de espera..'
          WHERE tb.str_estado_prevalidacion = 'PEN'
          AND tb.dtm_fecha_modificacion <= fecha_habil;
          -- Ejecutamos Tx
        COMMIT;

        marca_final := systimestamp;
        log_ejecucion(inestadolog => 'Proceso prenota Finaliza: ' || marca_final, indescripcionlog => 'Finaliza Verificación Días Hábiles - Estado prevalidación Beneficiario'
        , infechainicio => fecha_inicio, infechafinal => sysdate, outestadoejecucion => outestadoejecucion,
        outmensajeejecucion => outmensajeejecucion);
        -- Transacción exitosa
        outestadoejecucion := estado_ejecucion_exitosa;
        outmensajeejecucion := 'Transacción exitosa';
      EXCEPTION
        WHEN OTHERS THEN
           -- Código y mensaje del error
           outestadoejecucion := sqlcode;
           outmensajeejecucion := substr(sqlerrm, 1, 200);
    END beneficiario_dias_habiles;

	PROCEDURE sincronizar (outestadoejecucion 	OUT PLS_INTEGER,
								  outmensajeejecucion	OUT VARCHAR2
								 )
	IS
	    MARCA_INICIAL_1    DATE:=SYSDATE;
		MARCA_INICIAL_ET1 DATE;
		MARCA_INICIAL_ET2 DATE;
		MARCA_INICIAL_ET3 DATE;
		MARCA_INICIAL_ET4 DATE;
		MARCA_INICIAL_ET5 DATE;
		TIEMPO_FINAL  NUMBER:=0;

        marca_inicial   TIMESTAMP := systimestamp;
        marca_final     TIMESTAMP;
        tiempo_total    NUMBER;

	BEGIN
		DBMS_APPLICATION_INFO.SET_MODULE ( module_name => 'ptr_pkg_sincronizar_pagos',
                                       action_name => 'sincronizar');

		log_ejecucion (inestadolog 			 => 'INICIO SINCRONIZACION V.1.2',
						indescripcionlog		 => TO_CHAR(MARCA_INICIAL,'DD/MM/YYYY HH24:MI:SS'),
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

        MARCA_INICIAL_ET1:=SYSDATE;

		log_ejecucion (inestadolog 			 => 'INICIO ETAPA UNO ',
						indescripcionlog		 => TO_CHAR(MARCA_INICIAL_ET1,'DD/MM/YYYY HH24:MI:SS'),
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		etapa_uno (outestadoejecucion 	=> outestadoejecucion,
					  outmensajeejecucion	=> outmensajeejecucion
					 );

		log_ejecucion (inestadolog 			 => 'INICIO ETAPA UNO ',
						indescripcionlog		 => 'TIEMPO ET1: '||TRUNC((SYSDATE-MARCA_INICIAL_ET1)*1440,2)||' MIN',
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		MARCA_INICIAL_ET2:=SYSDATE;

		log_ejecucion (inestadolog 			 => 'INICIO ETAPA DOS ',
						indescripcionlog		 => TO_CHAR(MARCA_INICIAL_ET2,'DD/MM/YYYY HH24:MI:SS'),
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		etapa_dos (outestadoejecucion 	=> outestadoejecucion,
					  outmensajeejecucion	=> outmensajeejecucion
					 );

		log_ejecucion (inestadolog 			 => 'FIN ETAPA DOS ',
						indescripcionlog		 => 'TIEMPO ET2: '||TRUNC((SYSDATE-MARCA_INICIAL_ET2)*1440,2)||' MIN',
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;



		MARCA_INICIAL_ET3:=SYSDATE;

		log_ejecucion (inestadolog 			 => 'INICIO ETAPA TRES ',
						indescripcionlog		 => TO_CHAR(MARCA_INICIAL_ET3,'DD/MM/YYYY HH24:MI:SS'),
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		etapa_tres (outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		log_ejecucion (inestadolog 			 => 'FIN ETAPA TRES ',
						indescripcionlog		 => 'TIEMPO ET3: '||TRUNC((SYSDATE-MARCA_INICIAL_ET3)*1440,2)||' MIN',
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		MARCA_INICIAL_ET4:=SYSDATE;

		log_ejecucion (inestadolog 			 => 'INICIO ETAPA CUATRO ',
						indescripcionlog		 => TO_CHAR(MARCA_INICIAL_ET4,'DD/MM/YYYY HH24:MI:SS'),
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		etapa_cuatro (outestadoejecucion 	=> outestadoejecucion,
						  outmensajeejecucion	=> outmensajeejecucion
						 );

		log_ejecucion (inestadolog 			 => 'FIN ETAPA CUATRO ',
						indescripcionlog		 => 'TIEMPO ET4: '||TRUNC((SYSDATE-MARCA_INICIAL_ET4)*1440,2)||' MIN',
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		MARCA_INICIAL_ET5:=SYSDATE;

		log_ejecucion (inestadolog 			 => 'INICIO ETAPA CINCO ',
						indescripcionlog		 => TO_CHAR(MARCA_INICIAL_ET5,'DD/MM/YYYY HH24:MI:SS'),
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		etapa_cinco (outestadoejecucion	  => outestadoejecucion,
						 outmensajeejecucion   => outmensajeejecucion
						);

        log_ejecucion (inestadolog 			 => 'FIN ETAPA CINCO ',
						indescripcionlog		 => 'TIEMPO ET5: '||TRUNC((SYSDATE-MARCA_INICIAL_ET5)*1440,2)||' MIN',
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

		IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		COMMIT;

            -- Prevalidación Beneficiario
    prevalidacion_beneficiario(outestadoejecucion => outestadoejecucion, outmensajeejecucion => outmensajeejecucion);

        IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		COMMIT;

    -- Prevalidación Beneficiario : Dias habiles
    beneficiario_dias_habiles(outestadoejecucion => outestadoejecucion, outmensajeejecucion => outmensajeejecucion);

        IF outestadoejecucion <> estado_ejecucion_exitosa
		THEN
			RAISE excepcion_personalizada;
		END IF;

		COMMIT;

		outestadoejecucion := estado_ejecucion_exitosa;

        marca_final := systimestamp;
        tiempo_total := ( ( extract(HOUR FROM marca_final) * 3600 ) + ( extract(MINUTE FROM marca_final) * 60 ) + extract(SECOND FROM
        marca_final) ) - ( ( extract(HOUR FROM marca_inicial) * 3600 ) + ( extract(MINUTE FROM marca_inicial) * 60 ) + extract(SECOND
        FROM marca_inicial) );

		log_ejecucion (inestadolog 			 => 'FIN SINCRONIZACION MIN',
						indescripcionlog		 => 'TIEMPO: '||TRUNC((SYSDATE-MARCA_INICIAL_1)*1440,2)||' MIN',
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );
        log_ejecucion (inestadolog 			 => 'FIN SINCRONIZACION SEG',
						indescripcionlog		 => 'tiempo segundos segundos: ' || tiempo_total,
						infechainicio			 => SYSDATE,
						infechafinal			 => SYSDATE,
						outestadoejecucion	 => outestadoejecucion,
						outmensajeejecucion	 => outmensajeejecucion
					  );

	EXCEPTION
		WHEN excepcion_personalizada
		THEN
			ROLLBACK;

			log_ejecucion (inestadolog 			 => 'ERROR',
								indescripcionlog		 => outmensajeejecucion,
								infechainicio			 => SYSDATE,
								infechafinal			 => SYSDATE,
								outestadoejecucion	 => outestadoejecucion,
								outmensajeejecucion	 => outmensajeejecucion
							  );
		WHEN OTHERS
		THEN
			ROLLBACK;

			log_ejecucion (
				inestadolog 			 => 'ERROR',
				indescripcionlog		 =>	 SUBSTR (SQLERRM, 1, 200)
												 || DBMS_UTILITY.format_error_backtrace,
				infechainicio			 => SYSDATE,
				infechafinal			 => SYSDATE,
				outestadoejecucion	 => outestadoejecucion,
				outmensajeejecucion	 => outmensajeejecucion);

			outestadoejecucion := SQLCODE;
			outmensajeejecucion :=
				SUBSTR (SQLERRM, 1, 200) || DBMS_UTILITY.format_error_backtrace;
	END sincronizar;

END ptr_pkg_sincronizar_pagos;
/