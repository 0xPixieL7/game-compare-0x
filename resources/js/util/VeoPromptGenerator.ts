/**
 * Generates structured JSON prompts for Google VEO 3 / AI Video Generators,
 * following the "VEO 3 JSON Prompting" pattern for high-end ads.
 */
export function generateVeoUnboxingPrompt(gameName: string) {
  return {
    "version": "VEO_3_JSON",
    "shot_sequence": [
      {
        "time": "0s-2s",
        "description": `Close-up shot of a sleek, futuristic digital vault in a dark, atmospheric tech laboratory. The vault features glowing blue neon circuitry and the text '${gameName}' etched in light.`,
        "camera": {
          "type": "Macro",
          "movement": "Slow Orbit",
          "focal_length": "85mm"
        },
        "lighting": "Cyberpunk, moody teal and violet rim lighting",
        "material_properties": "Brushed titanium, crystalline glass"
      },
      {
        "time": "2s-4s",
        "description": `The vault panels shift and retract with hydraulic precision. A burst of volumetric light and data particles erupts from the center as the game metadata of '${gameName}' starts to resolve.`,
        "camera": {
          "type": "Wide",
          "movement": "Quick Zoom-out",
          "focal_length": "24mm"
        },
        "lighting": "Explosive bloom, lens flares, high exposure",
        "fx": "Volumetric fog, liquid metal transformation"
      },
      {
        "time": "4s-6s",
        "description": `The final reveal: The core of the signal illuminates the room, displaying a cinematic holographic 3D cover of '${gameName}' floating in the center of the laboratory.`,
        "camera": {
          "type": "Medium",
          "movement": "Static tilt-up",
          "focal_length": "35mm"
        }
      }
    ]
  };
}
