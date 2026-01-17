<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment\Traits;

/**
 * Trait for categorizing video types by name.
 *
 * Used across all enrichment jobs (Steam, IGDB, TGDB, etc.) for consistent
 * video type categorization into collection types.
 */
trait CategorizesVideoTypes
{
    /**
     * Categorize video by name into collection types.
     *
     * Categories:
     * - gameplay: Gameplay footage, playthroughs
     * - launch_trailers: Release/launch trailers
     * - announcement_trailers: Reveal/announcement trailers
     * - cinematic_trailers: CGI/cinematic trailers
     * - story_trailers: Story/narrative trailers
     * - trailers: Generic trailers
     * - dlc_content: DLC/expansion/update videos
     * - tutorials: How-to guides, tutorials
     * - previews: Teasers, sneak peeks, previews
     * - accolades: Reviews, awards, accolades
     * - features: Feature highlights
     * - promotional: Default category
     */
    protected function categorizeVideoType(string $name): string
    {
        $nameLower = strtolower($name);

        // Gameplay videos
        if (str_contains($nameLower, 'gameplay') || str_contains($nameLower, 'game play')) {
            return 'gameplay';
        }

        // Trailers - various types (order matters - specific before generic)
        if (str_contains($nameLower, 'launch trailer') || str_contains($nameLower, 'release trailer')) {
            return 'launch_trailers';
        }
        if (str_contains($nameLower, 'announce') || str_contains($nameLower, 'reveal')) {
            return 'announcement_trailers';
        }
        if (str_contains($nameLower, 'cinematic') || str_contains($nameLower, 'cgi')) {
            return 'cinematic_trailers';
        }
        if (str_contains($nameLower, 'story') || str_contains($nameLower, 'narrative')) {
            return 'story_trailers';
        }
        if (str_contains($nameLower, 'trailer')) {
            return 'trailers';
        }

        // DLC/Expansion content
        if (str_contains($nameLower, 'dlc') || str_contains($nameLower, 'expansion') || str_contains($nameLower, 'update')) {
            return 'dlc_content';
        }

        // Tutorials/How-to
        if (str_contains($nameLower, 'tutorial') || str_contains($nameLower, 'how to') || str_contains($nameLower, 'guide')) {
            return 'tutorials';
        }

        // Previews/Teasers
        if (str_contains($nameLower, 'preview') || str_contains($nameLower, 'teaser') || str_contains($nameLower, 'sneak')) {
            return 'previews';
        }

        // Reviews/Accolades
        if (str_contains($nameLower, 'review') || str_contains($nameLower, 'accolade') || str_contains($nameLower, 'award')) {
            return 'accolades';
        }

        // Features/Highlights
        if (str_contains($nameLower, 'feature') || str_contains($nameLower, 'highlight')) {
            return 'features';
        }

        // Default to general promotional
        return 'promotional';
    }

    /**
     * Get all available video categories.
     *
     * @return array<string>
     */
    protected function getVideoCategories(): array
    {
        return [
            'gameplay',
            'launch_trailers',
            'announcement_trailers',
            'cinematic_trailers',
            'story_trailers',
            'trailers',
            'dlc_content',
            'tutorials',
            'previews',
            'accolades',
            'features',
            'promotional',
        ];
    }
}
