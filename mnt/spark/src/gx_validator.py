import great_expectations as ge
import os
import json
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

def validate_spark_df(df, json_path: str, output_path: str):
    """
    Valida um DataFrame Spark usando Great Expectations e salva o resultado em JSON.

    Args:
        df: DataFrame Spark a ser validado
        json_path (str): caminho do arquivo de expectativas (JSON)
        output_path (str): diretório onde salvar o resultado
    
    Returns:
        str: caminho do arquivo de validação gerado
    """
    logger.info("Iniciando a validação...")
    ge_path = "/opt/great_expectations/gx/"
    # Pegar o contexto ativo
    context = ge.get_context(ge_path)

    # Nome da suite baseado no arquivo JSON
    suite_name = os.path.splitext(os.path.basename(json_path))[0]

    # Criar Expectation Suite se não existir
    try:
        suite = context.get_expectation_suite(suite_name)
    except ge.exceptions.DataContextError:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    # Validar runtime batch
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[{
            "batch_data": df,
            "expectation_suite_name": suite_name
        }]
    )

    # Salvar resultado
    validation_file = os.path.join(
        output_path,
        f"validation_result_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    with open(validation_file, "w") as f:
        json.dump(results.to_json_dict(), f, indent=2)

    logger.info(f"Validação concluída. Arquivo salvo em: {validation_file}")
    return validation_file
