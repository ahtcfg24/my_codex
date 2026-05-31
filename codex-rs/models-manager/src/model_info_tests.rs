use super::*;
use crate::ModelsManagerConfig;
use pretty_assertions::assert_eq;

#[test]
fn deepseek_models_have_explicit_metadata() {
    let model = model_info_from_slug("deepseek-v4-pro");

    assert_eq!(model.slug, "deepseek-v4-pro");
    assert_eq!(model.used_fallback_model_metadata, false);
    assert_eq!(model.context_window, Some(DEEPSEEK_CONTEXT_WINDOW_TOKENS));
    assert_eq!(
        model.max_context_window,
        Some(DEEPSEEK_CONTEXT_WINDOW_TOKENS)
    );
    assert_eq!(model.supports_parallel_tool_calls, true);
    assert_eq!(model.input_modalities, vec![InputModality::Text]);
    assert!(
        model
            .description
            .as_deref()
            .is_some_and(|description| description.contains("384K max output"))
    );
}

#[test]
fn mimo_models_have_explicit_metadata() {
    let model = model_info_from_slug("mimo-v2.5-pro");

    assert_eq!(model.slug, "mimo-v2.5-pro");
    assert_eq!(model.used_fallback_model_metadata, false);
    assert_eq!(model.context_window, Some(MIMO_PRO_CONTEXT_WINDOW_TOKENS));
    assert_eq!(
        model.max_context_window,
        Some(MIMO_PRO_CONTEXT_WINDOW_TOKENS)
    );
    assert_eq!(model.supports_parallel_tool_calls, true);
    assert_eq!(model.supports_search_tool, true);
    assert_eq!(model.input_modalities, vec![InputModality::Text]);
    assert!(
        model
            .description
            .as_deref()
            .is_some_and(|description| description.contains("128K max output"))
    );
}

#[test]
fn reasoning_summaries_override_true_enables_support() {
    let model = model_info_from_slug("unknown-model");
    let config = ModelsManagerConfig {
        model_supports_reasoning_summaries: Some(true),
        ..Default::default()
    };

    let updated = with_config_overrides(model.clone(), &config);
    let mut expected = model;
    expected.supports_reasoning_summaries = true;

    assert_eq!(updated, expected);
}

#[test]
fn reasoning_summaries_override_false_does_not_disable_support() {
    let mut model = model_info_from_slug("unknown-model");
    model.supports_reasoning_summaries = true;
    let config = ModelsManagerConfig {
        model_supports_reasoning_summaries: Some(false),
        ..Default::default()
    };

    let updated = with_config_overrides(model.clone(), &config);

    assert_eq!(updated, model);
}

#[test]
fn reasoning_summaries_override_false_is_noop_when_model_is_false() {
    let model = model_info_from_slug("unknown-model");
    let config = ModelsManagerConfig {
        model_supports_reasoning_summaries: Some(false),
        ..Default::default()
    };

    let updated = with_config_overrides(model.clone(), &config);

    assert_eq!(updated, model);
}

#[test]
fn model_context_window_override_clamps_to_max_context_window() {
    let mut model = model_info_from_slug("unknown-model");
    model.context_window = Some(273_000);
    model.max_context_window = Some(400_000);
    let config = ModelsManagerConfig {
        model_context_window: Some(500_000),
        ..Default::default()
    };

    let updated = with_config_overrides(model.clone(), &config);
    let mut expected = model;
    expected.context_window = Some(400_000);

    assert_eq!(updated, expected);
}

#[test]
fn model_context_window_uses_model_value_without_override() {
    let mut model = model_info_from_slug("unknown-model");
    model.context_window = Some(273_000);
    model.max_context_window = Some(400_000);
    let config = ModelsManagerConfig::default();

    let updated = with_config_overrides(model.clone(), &config);

    assert_eq!(updated, model);
}
